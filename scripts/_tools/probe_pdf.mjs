// 只读探针：用 dsh-wen 自带 PDF 解析器抽取分页文本（带页码分隔）
// 用法: node scripts/_tools/probe_pdf.mjs <pdf路径> <输出txt路径>
import fs from 'node:fs'
import { pathToFileURL } from 'node:url'

const PLUGIN = 'D:/DeepSeekHarness/dsh_data/profiles/web/node_modules/dsh-wen/'
const { readPdfText } = await import(pathToFileURL(PLUGIN + 'pdf.js').href)

const [pdfPath, outPath] = process.argv.slice(2)
if (!pdfPath || !outPath) { console.error('usage: probe_pdf.mjs <pdf> <out.txt>'); process.exit(2) }

const res = readPdfText(fs.readFileSync(pdfPath))
let out = ''
if (typeof res === 'string') out = res
else if (Array.isArray(res.sections)) {
  out = res.sections.map(s => `\n\n===== PAGE ${s.page} =====\n${s.text || ''}`).join('')
} else if (Array.isArray(res.pages)) {
  out = res.pages.map((p, i) => `\n\n===== PAGE ${i + 1} =====\n${p.text || ''}`).join('')
} else out = JSON.stringify(res).slice(0, 4000)

fs.writeFileSync(outPath, out, 'utf8')
console.log(JSON.stringify({ sections: res.sections?.length ?? null, chars: out.length }))
