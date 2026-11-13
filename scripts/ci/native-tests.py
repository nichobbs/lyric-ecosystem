#!/usr/bin/env python3
"""Run a package's [project.tests] on --target native.

`lyric test --manifest ... --target native` is refused (lyric-lang #6815 item 3(b)), so this does
by hand what that command would do: copy the package (and workspace siblings it depends on) to a
work directory, rewrite each `@test_module` the way Lyric.TestSynth.synthesizeNative does (each
`test "t" { body }` becomes a function, plus a `main` that calls them in order and prints TAP),
add it to [project.packages] of a copy of the manifest, `lyric build --manifest --target native`,
and run the binary. One binary per test file. Test bodies are byte-identical.

usage: native_tests.py <lyric> <repo-root> <package-dir> <workdir> [--features a,b] [--only stem]
"""
import os, re, shutil, subprocess, sys

def synth(src):
    out, titles = [], []
    lines = src.split("\n")
    for ln in lines:
        m = re.match(r'^test\s+"((?:[^"\\]|\\.)*)"\s*\{\s*$', ln)
        if m:
            titles.append(m.group(1))
            out.append("func __native_test_%d(): Unit {" % (len(titles) - 1))
        elif ln.strip() == "@test_module":
            continue
        else:
            out.append(ln)
    body = "\n".join(out)
    if "func main(" in body:
        raise SystemExit("test module defines main")
    main = ["", "func main(): Int {", '  __nt_console.println("1..%d")' % len(titles)]
    for i, t in enumerate(titles):
        main.append("  __native_test_%d()" % i)
        main.append('  __nt_console.println("ok %d - %s")' % (i + 1, t))
    main.append('  __nt_console.println("# pass  %d")' % len(titles))
    main += ["  0", "}", ""]
    # the import goes after the package line
    body = re.sub(r'^(package [^\n]*\n)', r'\1\nimport Std.Console as __nt_console\n', body, count=1, flags=re.M)
    return body + "\n".join(main), len(titles)

def main():
    lyric, root, pkg, work = sys.argv[1:5]
    feats, only = None, None
    rest = sys.argv[5:]
    while rest:
        if rest[0] == "--features": feats = rest[1]
        elif rest[0] == "--only": only = rest[1]
        rest = rest[2:]
    root = os.path.abspath(root)
    if os.path.exists(work): shutil.rmtree(work)
    # copy the whole workspace minus build outputs, so workspace deps resolve the same way
    ign = shutil.ignore_patterns("bin", ".lyric-test", ".git", "obj", "*.dll", "*.jar")
    shutil.copytree(root, os.path.join(work, os.path.basename(root)), ignore=ign, symlinks=True)
    wroot = os.path.join(work, os.path.basename(root))
    # the workspace overrides point at ../lyric-lang
    os.symlink(os.path.join(os.path.dirname(root), "lyric-lang"), os.path.join(work, "lyric-lang"))
    wpkg = os.path.join(wroot, pkg)
    manifest = open(os.path.join(wpkg, "lyric.toml")).read()
    # [project.tests] entries
    m = re.search(r'^\[project\.tests\]\n(.*?)(?=^\[|\Z)', manifest, flags=re.M | re.S)
    tests = re.findall(r'^"([^"]+)"\s*=\s*"([^"]+)"', m.group(1), flags=re.M)
    base = re.sub(r'^\[project\.tests\]\n.*?(?=^\[|\Z)', '', manifest, flags=re.M | re.S)
    total, failed = 0, 0
    for name, path in tests:
        stem = os.path.splitext(os.path.basename(path))[0]
        if only and stem != only: continue
        src = open(os.path.join(wpkg, path)).read()
        if re.search(r'^@cfg\(feature = "integration"\)', src, flags=re.M) and not feats:
            continue
        body, n = synth(src)
        spath = os.path.join(os.path.dirname(path), "__native_" + os.path.basename(path))
        open(os.path.join(wpkg, spath), "w").write(body)
        man = re.sub(r'^(\[project\.packages\]\n)', r'\1"%s" = "%s"\n' % (name, spath), base, count=1, flags=re.M)
        man = re.sub(r'^output\s*=.*\n', '', man, flags=re.M)
        man = re.sub(r'^output_assembly\s*=.*\n', '', man, flags=re.M)
        open(os.path.join(wpkg, "lyric.toml"), "w").write(man)
        exe = os.path.join(work, stem)
        cmd = [lyric, "build", "--manifest", os.path.join(pkg, "lyric.toml"), "--target", "native", "-o", exe]
        if feats: cmd += ["--features", feats]
        print("=== %s/%s (%d tests) ===" % (pkg, path, n), flush=True)
        r = subprocess.run(cmd, cwd=wroot)
        if r.returncode != 0 or not os.path.exists(exe):
            print("BUILD FAILED: %s" % path, flush=True); failed += n; total += n; continue
        r = subprocess.run([exe], cwd=wroot, capture_output=True, text=True, timeout=600)
        sys.stdout.write(r.stdout); sys.stderr.write(r.stderr)
        ok = len(re.findall(r'^ok \d+ - ', r.stdout, flags=re.M))
        total += n; failed += n - ok
        if r.returncode != 0: print("EXIT %d: %s" % (r.returncode, path), flush=True)
        os.remove(os.path.join(wpkg, spath))
    print("native: %d test(s), %d passed, %d failed" % (total, total - failed, failed))
    sys.exit(1 if failed else 0)

main()
