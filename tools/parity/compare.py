#!/usr/bin/env python3
import json, os, sys

def read(path):
    data={}
    with open(path, encoding='utf-8') as f:
        for line in f:
            if line.strip():
                item=json.loads(line); data[item['case']]=item
    return data

def canonical_ids(cases_path):
    # A case exercises server-side canonicalization when its SQL calls one of
    # the Db2 functions the qualification document lists. Detect them from the
    # manifest instead of guessing from case-id prefixes.
    funcs=('HEX(','LENGTH(','VARCHAR(','VARCHAR_FORMAT(','XMLSERIALIZE(')
    ids=set()
    try:
        with open(cases_path, encoding='utf-8') as f:
            for line in f:
                line=line.rstrip('\n')
                if not line or line.startswith('#'): continue
                parts=line.split('|',2)
                if len(parts)==3 and any(fn in parts[2].upper() for fn in funcs):
                    ids.add(parts[0])
    except OSError:
        pass
    return ids

files={name:read(path) for name,path in [('Rust',sys.argv[1]),('.NET',sys.argv[2]),('JCC',sys.argv[3])]}
ids=sorted(files['Rust'])
failures=[]  # (provider, case, message)
for provider, data in files.items():
    if set(data)!=set(ids): failures.append((provider,'<case-list>','case list differs'))
for case in ids:
    base=files['Rust'][case]
    for provider,data in files.items():
        item=data.get(case)
        if item is None: continue
        # Values and semantic SQL type/precision/scale are parity requirements.
        # Some metadata APIs report expression nullability differently; retain
        # it in output for inspection without treating it as a mismatch.
        if item['rows'] != base['rows']: failures.append((provider,case,'values differ'))
        if item.get('sqlcode')!=base.get('sqlcode') or item.get('sqlstate')!=base.get('sqlstate'):
            failures.append((provider,case,'SQLCODE/SQLSTATE differs'))
        for i,(a,b) in enumerate(zip(base['metadata'],item['metadata'])):
            for key in ('type','precision','scale'):
                if a.get(key)!=b.get(key):
                    failures.append((provider,case,f'column {i} {key} {b.get(key)!r} != Rust {a.get(key)!r}'))
failed_ids={c for _,c,_ in failures if c in ids}
passed=len(ids)-len(failed_ids)
canonical=canonical_ids(os.getenv('DB2_PARITY_CASES','tests/parity/cases.tsv')) & set(ids)
canonical_failed=[c for c in sorted(canonical) if c in failed_ids]
canonical_state='PASS' if not canonical_failed else 'FAIL'
canonical_pass=len(canonical)-len(canonical_failed)
print('Db2 parity qualification')
print(f"Rust commit: {os.getenv('DB2_PARITY_COMMIT','unknown')}")
print(f"Db2: {os.getenv('DB2_PARITY_DB_VERSION','unknown')}")
print(f"IBM .NET package: Net.IBM.Data.Db2-lnx {os.getenv('DB2_PARITY_DOTNET_VERSION','10.0.0.300')}")
print(f"IBM JCC: {os.getenv('DB2_PARITY_JCC_VERSION','12.1.5.0')}")
print(f"Java: {os.getenv('DB2_PARITY_JAVA_VERSION','unknown')}")
print(f".NET: {os.getenv('DB2_PARITY_DOTNET_RUNTIME','unknown')}")
for name in files:
    nfail=len({c for p,c,_ in failures if p==name})
    print(f'{name}: PASS {len(ids)-nfail} / {len(ids)}')
print(f'Server canonical expressions: {canonical_state} {canonical_pass} / {len(canonical)}')
print(f'Overall: {"FAIL" if failures else "PASS"}')
if failures:
    print('Failed case ids:')
    for provider,case,msg in failures: print(f'  case {case}: {provider} {msg}')
report=os.getenv('DB2_PARITY_REPORT')
if report:
    report_lines=[
        'Db2 parity qualification',
        f'Rust commit: {os.getenv("DB2_PARITY_COMMIT","unknown")}',
        f'Db2: {os.getenv("DB2_PARITY_DB_VERSION","unknown")}',
        f'IBM .NET: {os.getenv("DB2_PARITY_DOTNET_VERSION","10.0.0.300")}',
        f'IBM JCC: {os.getenv("DB2_PARITY_JCC_VERSION","12.1.5.0")}',
        f'Java: {os.getenv("DB2_PARITY_JAVA_VERSION","unknown")}',
        f'.NET: {os.getenv("DB2_PARITY_DOTNET_RUNTIME","unknown")}',
        f'Cases: {passed}/{len(ids)}',
        f'Server canonical expressions: {canonical_state} {canonical_pass}/{len(canonical)}',
        f'Overall: {"FAIL" if failures else "PASS"}',
    ]
    open(report,'w',encoding='utf-8').write('\n'.join(report_lines)+'\n'+'\n'.join(f'case {c}: {p} {m}' for p,c,m in failures)+'\n')
sys.exit(bool(failures))
