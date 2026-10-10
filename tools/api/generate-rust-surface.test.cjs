'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { parsePage } = require('./generate-rust-surface.cjs');

test('Rust API surface contains inherent methods, not methods reachable through Deref', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rust-api-surface-'));
  try {
    const directory = path.join(root, 'network', 'dss');
    fs.mkdirSync(directory, { recursive: true });
    const page = path.join(directory, 'struct.DssPayload.html');
    fs.writeFileSync(page, [
      '<pre class="rust item-decl">pub struct DssPayload { /* private fields */ }</pre>',
      '<h3 class="code-header">impl DssPayload</h3>',
      '<h4 class="code-header">pub fn as_slice(&amp;self) -&gt; &amp;[u8]</h4>',
      '<h4 class="code-header">pub fn try_into_vec(self) -&gt; Vec&lt;u8&gt;</h4>',
      '<h3 class="code-header">impl AsRef&lt;[u8]&gt; for DssPayload</h3>',
      '<h4 class="code-header">pub fn as_ref(&amp;self) -&gt; &amp;[u8]</h4>',
      '<h2 class="section-header" id="deref-methods-%5Bu8%5D">Methods from Deref</h2>',
      '<h4 class="code-header">pub fn array_windows(&amp;self)</h4>',
      '<h4 class="code-header">pub fn as_array(&amp;self)</h4>',
    ].join('\n'));

    const item = parsePage(page, root);
    assert.equal(item.path, 'network::dss::DssPayload');
    assert.deepEqual(item.methods, [
      'pub fn as_slice(&self) -> &[u8]',
      'pub fn try_into_vec(self) -> Vec<u8>',
    ]);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
