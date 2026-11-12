//! tests/fake_server_tests.l - the client against an in-process fake RESP
//! server, so the hermetic suite covers real sockets on both targets: the
//! handshake (AUTH, HELLO, SELECT), replies split across reads, a server that
//! closes in the middle of a reply, stale connections, garbage, timeouts and
//! a refused connection.
//!
//! The fake listens on a free loopback port and serves one connection at a
//! time on a background thread. It decodes each command with `Redis.Resp`,
//! records it, and lets the test's handler choose what goes back on the wire.
//! `stopFake` ends it by sending `FAKE.STOP` on a fresh connection, so a test
//! closes its own client first.
@test_module
package Redis.FakeServerTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.String as Str
import Std.Testing
import Std.Time
import Redis
import Redis.Resp
import Redis.Kernel as Kernel

// ─── The fake server ─────────────────────────────────────────────────────────
union Action {
  /// Write `reply` and keep the connection.
  case Send(reply: String)
  /// Write `reply`, then close the connection.
  case SendThenClose(reply: String)
  /// Write `reply` one byte at a time.
  case SendSlowly(reply: String)
  /// Write nothing for `millis` milliseconds, then close the connection.
  case Hang(millis: Int)
}

record Fake {
  port: Int
  log: List[String]
  worker: Kernel.Background
}

func commandWords(v: in RespValue): List[String] {
  val words: List[String] = newList()
  match v {
    case RArray(items) -> {
      for item in items {
        match asText(item) {
          case Some(t) -> words.add(t)
          case None -> words.add("<binary>")
        }
      }
    }
    case _ -> words.add("<not-a-command>")
  }
  words
}

func writeWire(sock: in Kernel.Sock, text: in String): Unit {
  val _ = Kernel.sockWrite(sock, encodeUtf8(text))
}

func writeSlowly(sock: in Kernel.Sock, text: in String): Unit {
  val all = encodeUtf8(text)
  var i = 0
  while i < all.length {
    val _ = Kernel.sockWrite(sock, all.slice(i, i + 1))
    i = i + 1
  }
}

// Serve one connection. False once FAKE.STOP was received.
func serveConnection(
  sock: in Kernel.Sock,
  index: in Int,
  handler: in (List[String], Int) -> Action,
  log: in List[String],
): Bool {
  var decoder = newDecoder()
  var open = true
  var keepServing = true
  while open {
    match Kernel.sockRead(sock, 4096) {
      case Err(_) -> {
        open = false
      }
      case Ok(chunk) -> {
        if chunk.length == 0 {
          open = false
        } else {
          val fed = feed(decoder, chunk)
          decoder = fed.decoder
          for v in fed.values {
            if open {
              val words = commandWords(v)
              if words.count > 0 and words[0] == "FAKE.STOP" {
                writeWire(sock, "+OK\r\n")
                keepServing = false
                open = false
              } else {
                log.add(Str.joinList(" ", words))
                match handler(words, index) {
                  case Send(reply) -> writeWire(sock, reply)
                  case SendThenClose(reply) -> {
                    writeWire(sock, reply)
                    open = false
                  }
                  case SendSlowly(reply) -> writeSlowly(sock, reply)
                  case Hang(millis) -> {
                    sleepMillis(millis)
                    open = false
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  Kernel.sockClose(sock)
  keepServing
}

func serve(server: in Kernel.ServerSock, handler: in (List[String], Int) -> Action, log: in List[String]): Unit {
  var running = true
  var index = 0
  while running {
    match Kernel.serverAccept(server) {
      case Err(_) -> {
        running = false
      }
      case Ok(sock) -> {
        running = serveConnection(sock, index, handler, log)
        index = index + 1
      }
    }
  }
  Kernel.serverClose(server)
}

func startFake(handler: in (List[String], Int) -> Action): Fake {
  match Kernel.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("the fake server cannot listen: " + m)
    case Ok(server) -> {
      val log: List[String] = newList()
      val port = Kernel.serverPort(server)
      val worker = Kernel.startBackground({ -> serve(server, handler, log) })
      Fake(port = port, log = log, worker = worker)
    }
  }
}

func stopFake(fake: in Fake): Unit {
  val stopper = Redis.lazy(Redis.configFor("127.0.0.1", fake.port))
  val _ = Redis.command(stopper, ["FAKE.STOP"])
  stopper.close()
  Kernel.waitBackground(fake.worker)
}

func urlOf(fake: in Fake): String {
  "redis://127.0.0.1:" + fake.port.toString()
}

func bulk(text: in String): String {
  "$" + encodeUtf8(text).length.toString() + "\r\n" + text + "\r\n"
}

// A small keyspace: PING, GET, SET, DEL, plus AUTH / HELLO / SELECT checked
// against `password` (and `user`, when not empty). Authentication is tracked
// per connection.
record MiniRedis {
  user: String
  password: String
  store: Map[String, String]
  authenticated: Map[Int, Bool]
}

func miniRedis(user: in String, password: in String): MiniRedis {
  MiniRedis(user = user, password = password, store = newMap(), authenticated = newMap())
}

func credentialsOk(mini: in MiniRedis, user: in String, password: in String): Bool {
  val wantedUser = if mini.user.length > 0 { mini.user } else { "default" }
  user == wantedUser and password == mini.password
}

func miniHandle(mini: in MiniRedis, words: in List[String], conn: in Int): Action {
  val name = words[0]
  if name == "AUTH" {
    val user = if words.count == 3 { words[1] } else { "default" }
    val password = words[words.count - 1]
    if credentialsOk(mini, user, password) {
      mini.authenticated[conn] = true
      return Send(reply = "+OK\r\n")
    }
    return Send(reply = "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
  }
  if name == "HELLO" {
    if words.count >= 5 {
      if not credentialsOk(mini, words[3], words[4]) {
        return Send(reply = "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
      }
      mini.authenticated[conn] = true
    }
    return Send(reply = "%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n")
  }
  if mini.password.length > 0 and not mini.authenticated.containsKey(conn) {
    return Send(reply = "-NOAUTH Authentication required.\r\n")
  }
  if name == "PING" {
    return Send(reply = "+PONG\r\n")
  }
  if name == "SELECT" {
    return Send(reply = "+OK\r\n")
  }
  if name == "SET" {
    mini.store[words[1]] = words[2]
    return Send(reply = "+OK\r\n")
  }
  if name == "GET" {
    if mini.store.containsKey(words[1]) {
      return Send(reply = bulk(mini.store[words[1]]))
    }
    return Send(reply = "$-1\r\n")
  }
  if name == "DEL" {
    if mini.store.containsKey(words[1]) {
      val _ = mini.store.remove(words[1])
      return Send(reply = ":1\r\n")
    }
    return Send(reply = ":0\r\n")
  }
  Send(reply = "-ERR unknown command '" + name + "'\r\n")
}

func startMini(mini: in MiniRedis): Fake {
  startFake({ words, conn -> miniHandle(mini, words, conn) })
}

func ok[T](r: in Result[T, RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOf[T](r: in Result[T, RedisError], what: in String): String {
  match r {
    case Ok(_) -> {
      assertTrue(false, what + " should have failed")
      ""
    }
    case Err(e) -> e.code()
  }
}

func openUrl(url: in String): RedisClient {
  match Redis.connect(url) {
    case Ok(c) -> c
    case Err(e) -> panic("connect failed: " + e.message())
  }
}

func expectValue(r: in Result[Option[String], RedisError], expected: in String, what: in String): Unit {
  match ok(r, what) {
    case Some(v) -> assertEqual(v, expected, what)
    case None -> assertTrue(false, what + ": expected a value, got none")
  }
}

// ─── Round trips ─────────────────────────────────────────────────────────────
test "commands round-trip through a socket" {
  val fake = startMini(miniRedis("", ""))
  val c = openUrl(urlOf(fake))
  ok(Redis.ping(c), "PING")
  ok(Redis.set(c, "k", "café\r\nline"), "SET")
  expectValue(Redis.get(c, "k"), "café\r\nline", "GET")
  assertEqualLong(ok(Redis.del(c, ["k"]), "DEL"), 1i64, "DEL")
  match ok(Redis.get(c, "k"), "GET") {
    case None -> ()
    case Some(_) -> assertTrue(false, "the key was deleted")
  }
  assertEqual(codeOf(Redis.command(c, ["NOSUCH"]), "an unknown command"), "SERVER_ERROR", "error reply")
  ok(Redis.ping(c), "PING after an error reply")
  assertEqualInt(c.connections(), 1, "one connection")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "PING", "no handshake command without credentials or database")
}

test "lazy connects on the first command; open connects at once" {
  val fake = startMini(miniRedis("", ""))
  val c = Redis.lazy(Redis.configFor("127.0.0.1", fake.port))
  assertFalse(c.isConnected(), "not connected before the first command")
  assertEqualInt(c.connections(), 0, "no connection yet")
  ok(Redis.ping(c), "PING")
  assertTrue(c.isConnected(), "connected by the first command")
  c.close()
  assertFalse(c.isConnected(), "closed")
  assertEqual(codeOf(Redis.ping(c), "PING on a closed client"), "CLIENT_CLOSED", "closed")
  stopFake(fake)
}

// ─── Handshake ───────────────────────────────────────────────────────────────
test "a URL password is sent with AUTH before anything else" {
  val fake = startMini(miniRedis("", "s3cret"))
  val c = openUrl("redis://:s3cret@127.0.0.1:" + fake.port.toString())
  ok(Redis.set(c, "k", "v"), "SET")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "AUTH s3cret", "AUTH comes first")
  assertEqual(fake.log[1], "SET k v", "then the command")
}

test "an ACL user is sent as AUTH user password, then SELECT" {
  val fake = startMini(miniRedis("app", "p:w@d"))
  val c = openUrl("redis://app:p%3Aw%40d@127.0.0.1:" + fake.port.toString() + "/7")
  ok(Redis.ping(c), "PING")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "AUTH app p:w@d", "AUTH with the user name")
  assertEqual(fake.log[1], "SELECT 7", "SELECT after AUTH")
  assertEqual(fake.log[2], "PING", "then the command")
}

test "a wrong password is AuthFailed and the message does not repeat it" {
  val fake = startMini(miniRedis("", "right"))
  match Redis.connect("redis://:hunter2@127.0.0.1:" + fake.port.toString()) {
    case Err(AuthFailed(m)) -> assertFalse(Str.contains(m, "hunter2"), "the password is not in the message")
    case Err(e) -> assertTrue(false, "expected AuthFailed, got " + e.code())
    case Ok(_) -> assertTrue(false, "a wrong password must not connect")
  }
  stopFake(fake)
}

test "a missing password surfaces the server's NOAUTH reply" {
  val fake = startMini(miniRedis("", "right"))
  val c = openUrl(urlOf(fake))
  match Redis.ping(c) {
    case Err(ServerError(m)) -> assertTrue(m.startsWith("NOAUTH"), "NOAUTH")
    case _ -> assertTrue(false, "an unauthenticated command must be a ServerError")
  }
  c.close()
  stopFake(fake)
}

test "RESP3 authenticates inside HELLO" {
  val fake = startMini(miniRedis("app", "pw"))
  val c = openUrl("redis://app:pw@127.0.0.1:" + fake.port.toString() + "/2?protocol=3")
  ok(Redis.ping(c), "PING")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "HELLO 3 AUTH app pw", "HELLO carries the credentials")
  assertEqual(fake.log[1], "SELECT 2", "SELECT after HELLO")
}

test "RESP3 with a password only authenticates as the default user" {
  val fake = startMini(miniRedis("", "pw"))
  val c = openUrl("redis://:pw@127.0.0.1:" + fake.port.toString() + "?protocol=3")
  ok(Redis.ping(c), "PING")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "HELLO 3 AUTH default pw", "HELLO names the default user")
}

test "a wrong password over RESP3 is AuthFailed" {
  val fake = startMini(miniRedis("app", "pw"))
  assertEqual(
    codeOf(Redis.connect("redis://app:bad@127.0.0.1:" + fake.port.toString() + "?protocol=3"), "connect"),
    "AUTH_FAILED",
    "HELLO with a wrong password"
  )
  stopFake(fake)
}

test "a server without HELLO refuses RESP3 with a ServerError" {
  val fake = startFake({ words, conn -> Send(reply = "-ERR unknown command 'HELLO'\r\n") })
  match Redis.connect(urlOf(fake) + "?protocol=3") {
    case Err(ServerError(m)) -> assertTrue(Str.contains(m, "RESP3"), "the message names RESP3")
    case Err(e) -> assertTrue(false, "expected ServerError, got " + e.code())
    case Ok(_) -> assertTrue(false, "the handshake must fail")
  }
  stopFake(fake)
}

test "a refused SELECT is a ServerError at open" {
  val fake = startFake({ words, conn -> Send(reply = "-ERR DB index is out of range\r\n") })
  assertEqual(codeOf(Redis.connect(urlOf(fake) + "/99"), "connect"), "SERVER_ERROR", "SELECT refused")
  stopFake(fake)
}

// ─── Replies on the wire ─────────────────────────────────────────────────────
test "a reply arriving one byte at a time is reassembled" {
  val fake = startFake({ words, conn -> SendSlowly(reply = "*2\r\n$5\r\nhello\r\n:42\r\n") })
  val c = openUrl(urlOf(fake))
  match ok(Redis.command(c, ["ANY"]), "the command") {
    case RArray(items) -> {
      assertEqualInt(items.count, 2, "two items")
      match items[1] {
        case RInt(n) -> assertEqualLong(n, 42i64, "the integer")
        case _ -> assertTrue(false, "the second item is an integer")
      }
    }
    case _ -> assertTrue(false, "the reply is an array")
  }
  c.close()
  stopFake(fake)
}

test "two replies in one write stay in step" {
  val fake = startFake(
    { words, conn -> if words[0] == "FIRST" { Send(reply = "+one\r\n") } else { Send(reply = "+two\r\n") } }
  )
  val c = openUrl(urlOf(fake))
  val first: slice[Byte] = encodeUtf8("FIRST")
  val second: slice[Byte] = encodeUtf8("SECOND")
  val replies = ok(c.pipeline([[first], [second]]), "pipeline")
  assertEqualInt(replies.count, 2, "two replies")
  match asText(replies[0]) {
    case Some(t) -> assertEqual(t, "one", "first reply")
    case None -> assertTrue(false, "the first reply is text")
  }
  match asText(replies[1]) {
    case Some(t) -> assertEqual(t, "two", "second reply")
    case None -> assertTrue(false, "the second reply is text")
  }
  c.close()
  stopFake(fake)
}

test "a RESP3 push message before the reply is skipped" {
  val fake = startFake({ words, conn -> Send(reply = ">2\r\n$10\r\ninvalidate\r\n*1\r\n$1\r\nk\r\n$5\r\nvalue\r\n") })
  val c = openUrl(urlOf(fake))
  expectValue(Redis.get(c, "k"), "value", "GET behind a push message")
  c.close()
  stopFake(fake)
}

test "bytes that are not RESP are a ProtocolViolation and drop the connection" {
  val fake = startFake(
    { words, conn ->
      if conn == 0 {
        Send(reply = "HTTP/1.1 400 Bad Request\r\n\r\n")
      } else {
        Send(reply = "+PONG\r\n")
      }
    }
  )
  val c = openUrl(urlOf(fake))
  assertEqual(codeOf(Redis.ping(c), "PING answered with HTTP"), "PROTOCOL_VIOLATION", "garbage")
  assertFalse(c.isConnected(), "the connection was dropped")
  ok(Redis.ping(c), "PING on a new connection")
  assertEqualInt(c.connections(), 2, "the client reconnected")
  c.close()
  stopFake(fake)
}

test "a reply of the wrong type is an UnexpectedReply" {
  val fake = startFake({ words, conn -> Send(reply = ":5\r\n") })
  val c = openUrl(urlOf(fake))
  assertEqual(codeOf(Redis.get(c, "k"), "GET answered with an integer"), "UNEXPECTED_REPLY", "wrong type")
  assertEqual(codeOf(Redis.ping(c), "PING answered with an integer"), "UNEXPECTED_REPLY", "wrong type")
  assertTrue(c.isConnected(), "a well-formed reply keeps the connection")
  c.close()
  stopFake(fake)
}

// ─── Broken connections ──────────────────────────────────────────────────────
test "a server closing in the middle of a reply is ConnectionLost, not retried" {
  val fake = startFake(
    { words, conn ->
      if conn == 0 {
        SendThenClose(reply = "$10\r\nabc")
      } else {
        Send(reply = "+PONG\r\n")
      }
    }
  )
  val c = openUrl(urlOf(fake))
  assertEqual(codeOf(Redis.get(c, "k"), "GET cut short"), "CONNECTION_LOST", "cut in the middle of a reply")
  assertFalse(c.isConnected(), "the broken connection was dropped")
  ok(Redis.ping(c), "PING on a new connection")
  assertEqualInt(c.connections(), 2, "the client reconnected for the next command")
  c.close()
  stopFake(fake)
  assertEqualInt(fake.log.count, 2, "the cut command was sent once")
  assertEqual(fake.log[0], "GET k", "the cut command")
  assertEqual(fake.log[1], "PING", "the next command")
}

test "a connection the server closed while idle is replaced and the command runs once" {
  val mini = miniRedis("", "")
  val fake = startFake(
    { words, conn ->
      if conn == 0 and words[0] == "PING" {
        SendThenClose(reply = "+PONG\r\n")
      } else {
        miniHandle(mini, words, conn)
      }
    }
  )
  val c = openUrl(urlOf(fake))
  ok(Redis.ping(c), "PING")
  sleepMillis(100)
  ok(Redis.set(c, "k", "v"), "SET on the stale connection")
  assertEqualInt(c.connections(), 2, "the client reconnected")
  expectValue(Redis.get(c, "k"), "v", "GET")
  c.close()
  stopFake(fake)
  assertEqualInt(fake.log.count, 3, "PING, SET and GET, each once")
  assertEqual(fake.log[1], "SET k v", "the repeated command reached the server once")
}

test "with the stale retry off the closed connection is reported" {
  val fake = startFake({ words, conn -> SendThenClose(reply = "+PONG\r\n") })
  val c = ok(Redis.open(Redis.withStaleRetry(Redis.configFor("127.0.0.1", fake.port), false)), "open")
  ok(Redis.ping(c), "PING")
  sleepMillis(100)
  assertEqual(codeOf(Redis.ping(c), "PING on the stale connection"), "CONNECTION_LOST", "reported")
  ok(Redis.ping(c), "PING on a new connection")
  c.close()
  stopFake(fake)
}

test "a silent server trips the I/O timeout" {
  val fake = startFake({ words, conn -> if conn == 0 { Hang(millis = 1500) } else { Send(reply = "+PONG\r\n") } })
  val c = ok(Redis.open(Redis.withTimeouts(Redis.configFor("127.0.0.1", fake.port), 2000, 200)), "open")
  val started = nowEpochMillis()
  assertEqual(codeOf(Redis.ping(c), "PING with no answer"), "CONNECTION_LOST", "timed out")
  assertTrue(nowEpochMillis() - started < 1200i64, "the timeout fired before the server gave up")
  assertFalse(c.isConnected(), "the timed-out connection was dropped")
  c.close()
  stopFake(fake)
}

test "a refused connection is ConnectFailed" {
  val fake = startMini(miniRedis("", ""))
  val port = fake.port
  stopFake(fake)
  match Redis.open(Redis.configFor("127.0.0.1", port)) {
    case Err(ConnectFailed(m)) -> assertTrue(
      Str.contains(m, "127.0.0.1:" + port.toString()),
      "the message names the endpoint"
    )
    case Err(e) -> assertTrue(false, "expected ConnectFailed, got " + e.code())
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
  val c = Redis.lazy(Redis.configFor("127.0.0.1", port))
  assertEqual(codeOf(Redis.ping(c), "PING with nothing listening"), "CONNECT_FAILED", "refused")
  assertEqual(codeOf(Redis.ping(c), "PING again"), "CONNECT_FAILED", "still refused, still an error value")
}

test "an unusable URL is InvalidConfig" {
  assertEqual(codeOf(Redis.connect("rediss://"), "a rediss URL without a host"), "INVALID_CONFIG", "no host")
  assertEqual(codeOf(Redis.connect("127.0.0.1:6379"), "a URL without a scheme"), "INVALID_CONFIG", "no scheme")
}
