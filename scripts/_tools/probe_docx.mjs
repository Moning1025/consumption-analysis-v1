// 只读探针：用 dsh-wen 自带解析器抽取 docx 的段落级结构（样式名 + 大纲级别 + 文本）
// 用法: node scripts/_tools/probe_docx.mjs <docx路径> <输出tsv路径>
import fs from 'node:fs'
import { pathToFileURL } from 'node:url'

const PLUGIN = 'D:/DeepSeekHarness/dsh_data/profiles/web/node_modules/dsh-wen/'
const { openZip } = await import(pathToFileURL(PLUGIN + 'zip.js').href)
const { parseXML, findAll, children, textOf } = await import(pathToFileURL(PLUGIN + 'xml.js').href)

const [docxPath, outPath] = process.argv.slice(2)
if (!docxPath || !outPath) { console.error('usage: probe_docx.mjs <docx> <out.tsv>'); process.exit(2) }

const zip = openZip(fs.readFileSync(docxPath))
const docXml = zip.getText('word/document.xml')
const stylesXml = zip.getText('word/styles.xml')
if (!docXml) { console.error('no word/document.xml'); process.exit(1) }

const findEl = (node, name) => findAll(node, name)[0]

// ---- 1. 样式表：styleId -> { name, basedOn, outline(1-based 或 null) }
const styleMap = new Map()
if (stylesXml) {
  const root = parseXML(stylesXml)
  for (const s of findAll(root, 'w:style')) {
    const id = s.attrs['w:styleId']
    const nameEl = findEl(s, 'w:name')
    const basedOn = findEl(s, 'w:basedOn')?.attrs['w:val'] || null
    const pPr = findEl(s, 'w:pPr')
    const ol = pPr && findEl(pPr, 'w:outlineLvl')
    styleMap.set(id, {
      name: nameEl?.attrs['w:val'] || '',
      basedOn,
      outline: ol ? Number(ol.attrs['w:val']) + 1 : null,
    })
  }
}

function resolve(id, depth = 0) {
  const s = styleMap.get(id)
  if (!s) return { name: id || '(none)', outline: null }
  if (s.outline || depth > 6 || !s.basedOn) return { name: s.name, outline: s.outline }
  const p = resolve(s.basedOn, depth + 1)
  return { name: s.name, outline: p.outline }
}

// ---- 2. 正体段落
const doc = parseXML(docXml)
const body = findEl(doc, 'w:body') || { children: [] }
const rows = []
let i = 0
for (const el of children(body)) {
  if (el.name !== 'w:p') continue
  i++
  const pPr = findEl(el, 'w:pPr')
  const styleId = (pPr && findEl(pPr, 'w:pStyle')?.attrs['w:val']) || ''
  const { name, outline } = resolve(styleId)
  const text = textOf(el).replace(/\s+/g, ' ').trim()
  if (!text) continue
  rows.push([i, styleId || '-', name || '-', outline ?? '-', text].join('\t'))
}

fs.writeFileSync(outPath, rows.join('\n'), 'utf8')
console.log(`paragraphs=${i} rows=${rows.length} styles=${styleMap.size}`)
