import requests
import streamlit as st
import streamlit.components.v1 as components

HTML_2_CANVAS_URL = "https://unpkg.com/html2canvas@1.4.1/dist/html2canvas.js"
JS_PDF_URL = "https://unpkg.com/jspdf@2.5.1/dist/jspdf.umd.min.js"


@st.cache_data
def load_unpkg(src: str) -> str:
    return requests.get(src).text


def getResults(container, pageName="vectordb_bench"):
    container.subheader("Get results")
    saveAsImage(container, pageName)
    saveAsPDF(container, pageName)


def saveAsImage(container, pageName):
    html2canvasJS = load_unpkg(HTML_2_CANVAS_URL)
    container.write()
    buttonText = "Save as Image"
    saveImageButton = container.button(buttonText)
    if saveImageButton:
        components.html(
            _make_capture_script(html2canvasJS, buttonText, pageName, as_pdf=False),
            height=0,
            width=0,
        )


def saveAsPDF(container, pageName):
    html2canvasJS = load_unpkg(HTML_2_CANVAS_URL)
    jspdfJS = load_unpkg(JS_PDF_URL)
    buttonText = "Save as PDF"
    savePDFButton = container.button(buttonText)
    if savePDFButton:
        components.html(
            _make_capture_script(html2canvasJS, buttonText, pageName, as_pdf=True, jspdf_js=jspdfJS),
            height=0,
            width=0,
        )


def _make_capture_script(html2canvas_js: str, button_text: str, page_name: str, as_pdf: bool, jspdf_js: str = ""):
    """Build the inline script that captures the main content and downloads as image or PDF."""
    opts = "allowTaint: false, useCORS: true, scrollY: -window.scrollY, scrollX: -window.scrollX"
    if as_pdf:
        return f"""
<script>{html2canvas_js}</script>
<script>{jspdf_js}</script>
<script>
(function() {{
  const html2canvas = window.html2canvas;
  const jsPDF = window.jspdf && window.jspdf.jsPDF;

  const streamlitDoc = window.parent.document;
  const stApp = streamlitDoc.querySelector('.main .block-container');
  if (!stApp) return;

  const buttons = Array.from(streamlitDoc.querySelectorAll('.stButton > button'));
  const btn = buttons.find(el => el.innerText === '{button_text}');
  if (btn) btn.innerText = 'Creating PDF...';

  const opts = {{ {opts} }};
  html2canvas(stApp, opts).then(function(canvas) {{
    const imgData = canvas.toDataURL('image/png');
    const pdf = new jsPDF('p', 'mm', 'a4');
    const pdfW = 210;
    const pdfH = 297;
    const imgW = pdfW;
    const imgH = (canvas.height * pdfW) / canvas.width;
    let heightLeft = imgH;
    let position = 0;

    pdf.addImage(imgData, 'PNG', 0, position, imgW, imgH);
    heightLeft -= pdfH;

    while (heightLeft > 0) {{
      position = heightLeft - imgH;
      pdf.addPage();
      pdf.addImage(imgData, 'PNG', 0, position, imgW, imgH);
      heightLeft -= pdfH;
    }}

    const dataUrl = pdf.output('datauristring');
    const parentDoc = window.parent.document;
    const a = parentDoc.createElement('a');
    a.href = dataUrl;
    a.download = '{page_name}.pdf';
    a.style.display = 'none';
    parentDoc.body.appendChild(a);
    a.click();
    setTimeout(function() {{ a.remove(); }}, 300);
    if (btn) btn.innerText = '{button_text}';
  }});
}})();
</script>"""
    else:
        return f"""
<script>{html2canvas_js}</script>
<script>
(function() {{
  const html2canvas = window.html2canvas;
  const streamlitDoc = window.parent.document;
  const stApp = streamlitDoc.querySelector('.main .block-container');
  if (!stApp) return;

  const buttons = Array.from(streamlitDoc.querySelectorAll('.stButton > button'));
  const btn = buttons.find(el => el.innerText === '{button_text}');
  if (btn) btn.innerText = 'Creating Image...';

  html2canvas(stApp, {{ {opts} }}).then(function(canvas) {{
    const a = document.createElement('a');
    a.href = canvas.toDataURL("image/png").replace("image/png", "image/octet-stream");
    a.download = '{page_name}.png';
    a.click();
    if (btn) btn.innerText = '{button_text}';
  }});
}})();
</script>"""
