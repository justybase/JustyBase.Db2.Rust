#!/usr/bin/env node
'use strict';
// Generate an offline public-API snapshot for the Rust driver from `rustdoc`
// HTML (no external tools, no network). Run `cargo doc --no-deps --offline`
// first, or pass --doc <dir>.
//
// Usage: node tools/api/generate-rust-surface.cjs [--check] [--doc target/doc]
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const root = path.resolve(__dirname, '..', '..');
const args = process.argv.slice(2);
const check = args.includes('--check');
const docIndex = args.indexOf('--doc');
const docRoot = path.resolve(root, docIndex >= 0 ? args[docIndex + 1] : path.join('target', 'doc'));
const crateDir = path.join(docRoot, 'justybase_db2');
const outFile = path.join(root, 'api', 'rust-surface.json');

const KINDS = ['struct', 'enum', 'trait', 'fn', 'type', 'constant', 'static', 'union', 'macro', 'primitive'];
const TOGGLE = /\b(?:Show|Hide)\s+\d+\s+(?:fields|variants|associated items)\b/gi;

function cleanHtml(fragment) {
  return fragment
    .replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, '&')
    .replace(/ⓘ/g, '')
    .replace(/\s+/g, ' ')
    .replace(TOGGLE, '')
    .trim();
}

function walk(dir, out = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(full, out);
    else if (entry.name.endsWith('.html') && entry.name !== 'index.html') out.push(full);
  }
  return out;
}

function parsePage(file, baseDir = crateDir) {
  const relative = path.relative(baseDir, file).split(path.sep);
  const fileName = relative.pop();
  const match = /^(struct|enum|trait|fn|type|constant|static|union|macro|primitive)\.(.+)\.html$/.exec(fileName);
  if (!match) return null;
  const [, kind, name] = match;
  const modulePath = relative.join('::');
  const itemPath = modulePath ? `${modulePath}::${name}` : name;

  const text = fs.readFileSync(file, 'utf8');
  const declaration = /<pre class="rust item-decl">([\s\S]*?)<\/pre>/.exec(text);
  // rustdoc also renders methods reachable through Deref in the same page.
  // Those methods are documented after the semantic `deref-methods` section;
  // they belong to the target type (for example `[u8]`), not this item. Stop
  // before that section so a rustdoc release cannot add slice methods to the
  // driver's API snapshot.
  const derefSection = /<(?:h[1-6]|section)\b[^>]*\bid="deref-methods(?:-[^"]*)?"/i.exec(text);
  const ownedApiHtml = derefSection ? text.slice(0, derefSection.index) : text;
  const headers = [...ownedApiHtml.matchAll(/<h[34] class="code-header">([\s\S]*?)<\/h[34]>/g)].map((m) => cleanHtml(m[1]));

  const methods = [];
  let currentImpl = null;
  for (const header of headers) {
    if (header.startsWith('impl ')) {
      // Inherent impls read "impl Db2Pool" (no " for Trait").
      currentImpl = header.includes(' for ') ? null : header;
      continue;
    }
    if (currentImpl && (header.startsWith('pub fn ') || header.startsWith('pub async fn '))) {
      methods.push(header.replace(/\s+/g, ' '));
    }
  }

  return {
    path: itemPath,
    kind,
    declaration: declaration ? cleanHtml(declaration[1]) : '',
    methods: methods.sort(),
  };
}

function main() {
  if (!fs.existsSync(crateDir)) {
    console.error(`FAIL: rustdoc output not found at ${crateDir}; run "cargo doc --no-deps --offline" first`);
    process.exit(2);
  }
  const items = walk(crateDir).map((file) => parsePage(file)).filter(Boolean)
    .sort((a, b) => a.path.localeCompare(b.path) || a.kind.localeCompare(b.kind));
  const surface = {
    generator: 'tools/api/generate-rust-surface.cjs',
    crate: 'justybase-db2',
    counts: Object.fromEntries(KINDS.map((kind) => [kind, items.filter((item) => item.kind === kind).length])),
    items,
  };
  const serialized = JSON.stringify(surface, null, 2) + '\n';
  const digest = crypto.createHash('sha256').update(serialized).digest('hex');
  surface.digest = digest;

  if (check) {
    if (!fs.existsSync(outFile)) {
      console.error(`FAIL: ${outFile} is missing; run the generator and commit it`);
      process.exit(1);
    }
    const committed = JSON.parse(fs.readFileSync(outFile, 'utf8'));
    if (committed.digest === digest && JSON.stringify(committed) === JSON.stringify(surface)) {
      console.log(`PASS: rust API surface unchanged (${items.length} items, ${digest.slice(0, 12)})`);
      return;
    }
    const before = new Map((committed.items ?? []).map((item) => [item.path, item]));
    const after = new Map(items.map((item) => [item.path, item]));
    const removed = [...before.keys()].filter((key) => !after.has(key));
    const added = [...after.keys()].filter((key) => !before.has(key));
    const changed = [...after.entries()].filter(([key, item]) => before.has(key)
      && JSON.stringify(before.get(key)) !== JSON.stringify(item)).map(([key]) => key);
    console.error('FAIL: rust public API surface changed');
    for (const key of removed) console.error(`  - removed: ${key}`);
    for (const key of added) console.error(`  + added: ${key}`);
    for (const key of changed) console.error(`  ~ changed: ${key}`);
    if (removed.length === 0 && added.length === 0 && changed.length === 0) {
      console.error('  (digest mismatch only; regenerate the snapshot)');
    }
    process.exit(1);
  }

  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(surface, null, 2) + '\n');
  console.log(`wrote ${path.relative(root, outFile)} with ${items.length} items (digest ${digest.slice(0, 12)})`);
}

if (require.main === module) main();

module.exports = { parsePage };
