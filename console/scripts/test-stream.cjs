const assert = require('node:assert/strict')
const fs = require('node:fs')
const vm = require('node:vm')
const ts = require('typescript')
const { performance } = require('node:perf_hooks')

async function main() {
  const output = ts.transpileModule(fs.readFileSync('lib/api/client.ts', 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText
  let streamController
  const body = new ReadableStream({ start(controller) { streamController = controller } })
  const exports = {}
  const logs = []
  vm.runInNewContext(output, {
    exports, process: { env: {} }, performance, Headers, TextDecoder, AbortController, AbortSignal, DOMException,
    console: { info(...args) { logs.push(args) } }, setTimeout, clearTimeout,
    fetch: async () => new Response(body, { headers: { 'Content-Type': 'text/event-stream' } }),
  })
  const tokens = []
  let complete = false
  const pending = exports.apiStreamRequest('/ceaser/chat/stream', { method: 'POST', body: {}, headers: { 'X-Request-Id': 'test-request' } }, {
    onToken: text => tokens.push(text), onComplete: () => { complete = true },
  })
  const encoder = new TextEncoder()
  streamController.enqueue(encoder.encode('event: token\r\ndata: "hel'))
  streamController.enqueue(encoder.encode('lo"\r\n\r\n'))
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(tokens, ['hello'], 'first content must arrive before stream closure')
  assert.equal(complete, false)
  streamController.enqueue(encoder.encode('event: diagnostics\ndata: {"request_id":"test-request","stage":"first_content_forwarded","endpoint_ttft_ms":123}\n\n'))
  streamController.enqueue(encoder.encode('event: token\ndata: " world"\n\nevent: complete\ndata: {"response":"hello world"}\n\n'))
  streamController.close()
  await pending
  assert.equal(tokens.join(''), 'hello world')
  assert.equal(complete, true)
  assert.equal(body.locked, false)
  const diagnostic = JSON.parse(logs.find(entry => entry[0] === '[CEASER BACKEND TIMING]')[1])
  assert.equal(diagnostic.request_id, 'test-request')
  assert.equal(diagnostic.endpoint_ttft_ms, 123)
  assert.equal(typeof diagnostic.browser_received_ms, 'number')
  assert.equal(JSON.parse(logs[0][1]).request_id, 'test-request')
  console.log('PASS: fragmented SSE, CRLF, incremental delivery, completion, reader cleanup, correlated diagnostics')
}
main().catch(error => { console.error(error); process.exitCode = 1 })
