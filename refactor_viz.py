import re
import os

INPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"
OUTPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

original_html = read_file(INPUT_FILE)

# Extract JS logic
js_match = re.search(r'<script type="module">([\s\S]*?)</script>', original_html)
if not js_match:
    print("Error: Could not find script module")
    exit(1)

js_content = js_match.group(1)

# --- JS Transformations ---

# 1. buildCommitNode template
js_content = re.sub(
    r'details\.className = "glass-2 rounded-3xl px-4 py-3 shadow-sm";',
    r'details.className = "tui-panel p-2 mb-2";',
    js_content
)
js_content = re.sub(
    r'<span class="chip inline-flex items-center gap-2 rounded-full px-2.5 py-1 text-\[11px\] font-semibold text-slate-700">',
    r'<span class="tui-tag">',
    js_content
)
js_content = re.sub(
    r'<summary class="cursor-pointer list-none">',
    r'<summary class="cursor-pointer list-none hover:text-green-400">',
    js_content
)

# 2. bucketToggleItem template
js_content = js_content.replace(
    'btn.className =
          "focus-ring flex items-start gap-3 rounded-2xl border border-slate-900/10 bg-white/70 px-3 py-2 text-left hover:bg-white";',
    'btn.className = "tui-btn-toggle flex items-start gap-3 text-left w-full mb-2";'
)
js_content = js_content.replace(
    'text-slate-900', 'text-green-500'
).replace(
    'text-slate-700', 'text-green-600'
).replace(
    'text-slate-500', 'text-green-800'
).replace(
    'text-slate-400', 'text-green-900'
)

# 3. Fix colors in JS
js_content = js_content.replace('"rgba(2,6,23,.55)"', '"#004400"') # axis labels
js_content = js_content.replace('"rgba(2,6,23,.12)"', '"#004400"') # axis lines
js_content = js_content.replace('"rgba(2,6,23,.06)"', '"#002200"') # split lines
js_content = js_content.replace('"rgba(255,255,255,0.95)"', '"#0a0a0a"') # tooltip bg
js_content = js_content.replace('textStyle: { color: "#0b1220" }', 'textStyle: { color: "#33ff00", fontFamily: "JetBrains Mono" }')
js_content = js_content.replace('borderColor: "rgba(255,255,255,0.8)"', 'borderColor: "#000"')

# 4. renderGroup template
js_content = js_content.replace(
    'class="mt-3 rounded-3xl border border-slate-900/10 bg-white/60 p-4"',
    'class="mt-3 tui-panel p-2"'
)
js_content = js_content.replace('class="chip mono', 'class="tui-tag mono')

# 5. renderStoryCards
js_content = re.sub(
    r'class="story-card.*?data-story-idx',
    r'class="story-card tui-panel p-2 mb-2 cursor-pointer hover:border-green-500 transition-colors" data-story-idx',
    js_content
)

# 6. ECharts overrides
echarts_theme = """
          textStyle: { fontFamily: 'JetBrains Mono' },
          backgroundColor: 'transparent',
"""
js_content = js_content.replace('chartTimeline.setOption({', 'chartTimeline.setOption({' + echarts_theme)
js_content = js_content.replace('chartStack.setOption({', 'chartStack.setOption({' + echarts_theme)
js_content = js_content.replace('chartDonut.setOption({', 'chartDonut.setOption({' + echarts_theme)
js_content = js_content.replace('chartBocpd.setOption({', 'chartBocpd.setOption({' + echarts_theme)

# 7. General class replacements in JS
js_content = js_content.replace("glass-2", "tui-panel")
js_content = js_content.replace("glass", "tui-panel")
js_content = js_content.replace("shadow-glow", "")
js_content = js_content.replace("rounded-3xl", "")
js_content = js_content.replace("rounded-2xl", "")
js_content = js_content.replace("rounded-xl", "")
js_content = js_content.replace("bg-white/70", "bg-black")
js_content = js_content.replace("bg-white", "bg-black")
js_content = js_content.replace("border-slate-900/10", "border-green-900")

# --- New HTML Structure ---

new_html = """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>FrankenSQLite Spec Evolution</title>
    <link rel="stylesheet" href="frankentui.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/highlight.js@11.9.0/styles/github-dark.min.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/diff2html/bundles/css/diff2html.min.css" />
    <style>
        /* TUI Overrides */
        :root {
            --bg-color: #050505;
            --fg-color: #33ff00;
            --dim-color: #004400;
            --accent-color: #bd00ff;
            --panel-bg: #0a0a0a;
            --border-color: #33ff00;
        }
        body { font-family: 'JetBrains Mono', monospace; background: var(--bg-color); color: var(--fg-color); overflow: hidden; }
        
        .tui-grid { display: grid; grid-template-columns: 320px 1fr; gap: 1rem; height: calc(100vh - 60px); padding: 1rem; }
        .tui-col-left { display: flex; flex-direction: column; gap: 1rem; overflow-y: auto; }
        .tui-col-right { display: flex; flex-direction: column; gap: 1rem; overflow-y: hidden; }
        
        .tui-panel { border: 1px solid var(--border-color); background: var(--panel-bg); position: relative; box-shadow: 0 0 5px rgba(51, 255, 0, 0.1); }
        .tui-panel::before { content: ""; position: absolute; top: -1px; left: -1px; right: -1px; bottom: -1px; border: 1px dashed var(--dim-color); pointer-events: none; z-index: 10; }
        .tui-panel-content { padding: 1rem; overflow: auto; position: relative; z-index: 20; height: 100%; }
        
        .tui-tag { display: inline-block; padding: 2px 6px; border: 1px solid var(--dim-color); font-size: 0.75rem; margin-right: 4px; color: var(--fg-color); }
        
        /* Compat shims */
        .hidden { display: none !important; }
        .flex { display: flex; }
        .flex-col { flex-direction: column; }
        .gap-2 { gap: 0.5rem; }
        .gap-3 { gap: 0.75rem; }
        .items-center { align-items: center; }
        .justify-between { justify-content: space-between; }
        .w-full { width: 100%; }
        .mt-2 { margin-top: 0.5rem; }
        .mt-4 { margin-top: 1rem; }
        .text-xs { font-size: 0.75rem; }
        .font-bold { font-weight: bold; }
        .font-semibold { font-weight: 600; }
        .text-sm { font-size: 0.875rem; }
        .truncate { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        
        /* Specific component overrides */
        .chart { min-height: 250px; width: 100%; }
        
        input[type="text"], select {
            background: #000; border: 1px solid var(--fg-color); color: var(--fg-color); padding: 4px 8px; font-family: inherit;
        }
        button {
            background: #000; border: 1px solid var(--fg-color); color: var(--fg-color); padding: 4px 12px; cursor: pointer; text-transform: uppercase; font-size: 0.75rem;
        }
        button:hover { background: var(--fg-color); color: #000; }
        
        /* Dock */
        .dock { position: fixed; bottom: 0; left: 0; right: 0; background: #000; border-top: 1px solid var(--fg-color); padding: 10px; z-index: 100; }
        .dock-canvas { height: 40px; width: 100%; }
        
        /* Markdown */
        .md { color: #ccc; font-size: 0.9rem; line-height: 1.6; }
        .md h1, .md h2, .md h3 { color: var(--fg-color); border-bottom: 1px dashed var(--dim-color); padding-bottom: 0.5rem; margin-top: 1.5rem; }
        .md code { color: #bd00ff; }
        .md pre { background: #111; padding: 1rem; border: 1px solid var(--dim-color); overflow-x: auto; }
        
        /* Scrollbar */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #000; }
        ::-webkit-scrollbar-thumb { background: var(--dim-color); }
        ::-webkit-scrollbar-thumb:hover { background: var(--fg-color); }
    </style>
</head>
<body class="tui-screen">
    <!-- Header -->
    <header class="tui-header flex justify-between items-center p-4 border-b border-green-900">
        <div class="flex items-center gap-4">
            <h1 class="text-xl font-bold text-green-500 uppercase tracking-widest">FrankenSQLite <span class="text-xs text-green-800">Spec Evolution</span></h1>
            <div class="flex gap-4 text-xs">
                <div>Commits: <span id="kpiCommits" class="text-white">-</span></div>
                <div>Groups: <span id="kpiGroups" class="text-white">-</span></div>
                <div>Lines: <span id="kpiLines" class="text-white">-</span></div>
            </div>
        </div>
        <div class="flex gap-2">
            <button id="btnFilters">Filters</button>
            <button id="btnGalaxy">Galaxy Brain</button>
            <a href="https://github.com/Dicklesworthstone/frankensqlite" target="_blank"><button>GitHub</button></a>
        </div>
    </header>

    <!-- Main Layout -->
    <div class="tui-grid">
        <!-- Sidebar -->
        <aside class="tui-col-left">
            <div class="tui-panel">
                <div class="tui-panel-content">
                    <div class="text-xs font-bold uppercase mb-2 border-b border-green-900 pb-1">Search</div>
                    <input id="q" type="text" class="w-full mb-4" placeholder="Search commits...">
                    
                    <div class="text-xs font-bold uppercase mb-2 border-b border-green-900 pb-1">Impact Filter</div>
                    <div class="flex justify-between text-xs mb-1"><span>Min Lines</span><span id="impactLabel">0</span></div>
                    <input id="impact" type="range" min="0" max="200" value="0" class="w-full mb-4">
                    
                    <div class="text-xs font-bold uppercase mb-2 border-b border-green-900 pb-1">Bucket Mode</div>
                    <div class="flex gap-2 mb-4">
                        <button id="modePrimary" class="flex-1">Primary</button>
                        <button id="modeMulti" class="flex-1">Multi</button>
                    </div>
                    
                    <div class="text-xs font-bold uppercase mb-2 border-b border-green-900 pb-1">Buckets</div>
                    <div id="bucketToggles" class="flex flex-col gap-1"></div>
                </div>
            </div>
            
            <div class="tui-panel flex-1">
                <div class="tui-panel-content">
                    <div class="text-xs font-bold uppercase mb-2 border-b border-green-900 pb-1">Commits</div>
                    <div id="commitList" class="flex flex-col gap-2"></div>
                </div>
            </div>
        </aside>

        <!-- Right Column -->
        <main class="tui-col-right">
            <!-- Timeline & Stats -->
            <div class="tui-panel" style="min-height: 300px;">
                <div class="tui-panel-content flex flex-col h-full">
                    <div class="flex justify-between items-center mb-2">
                        <div class="text-xs font-bold uppercase">Timeline &amp; Activity</div>
                        <div class="flex gap-2">
                            <select id="stackResolution">
                                <option value="commit">By Commit</option>
                                <option value="day">By Day</option>
                                <option value="hour">By Hour</option>
                            </select>
                            <select id="stackMetric">
                                <option value="groups">Groups</option>
                                <option value="lines">Lines</option>
                            </select>
                        </div>
                    </div>
                    <div class="grid grid-cols-2 gap-4 flex-1">
                        <div id="timelineChart" class="chart"></div>
                        <div id="stackChart" class="chart"></div>
                    </div>
                </div>
            </div>

            <!-- Doc Viewer -->
            <div class="tui-panel flex-1">
                <div class="tui-panel-content flex flex-col h-full">
                    <div class="flex justify-between items-center border-b border-green-900 pb-2 mb-2">
                        <div class="flex gap-2">
                            <button id="docTabSpec">Spec</button>
                            <button id="docTabDiff">Diff</button>
                            <button id="docTabMetrics">Metrics</button>
                            <button id="docTabSections">Sections</button>
                        </div>
                        <div id="docCommitTitle" class="text-xs truncate max-w-[400px]"></div>
                    </div>
                    
                    <div id="docMain" class="flex-1 overflow-hidden relative">
                        <div id="docLoading" class="hidden absolute inset-0 flex items-center justify-center bg-black/80 z-50">Loading...</div>
                        
                        <div id="docSpecView" class="h-full flex flex-col hidden">
                            <div class="flex justify-between mb-2">
                                <div class="flex gap-2">
                                    <button id="btnMiniMapToggle">Outline</button>
                                    <button id="btnStoryToggle">Story</button>
                                </div>
                            </div>
                            <div class="flex flex-1 overflow-hidden gap-4">
                                <nav id="miniMap" class="hidden w-64 overflow-y-auto border-r border-green-900 pr-2"></nav>
                                <div id="docRendered" class="flex-1 overflow-y-auto md pr-2"></div>
                                <aside id="storyRail" class="hidden w-64 overflow-y-auto border-l border-green-900 pl-2">
                                    <div id="storyCards"></div>
                                </aside>
                            </div>
                            <pre id="docRaw" class="hidden flex-1 overflow-y-auto p-2 codebox"></pre>
                        </div>

                        <div id="docDiffView" class="h-full flex flex-col hidden">
                            <div class="flex gap-2 mb-2">
                                <button id="btnCompareToggle">A/B Compare</button>
                                <button id="btnDiffLayout">Side-by-Side</button>
                            </div>
                            <div id="abCompareBar" class="hidden flex gap-2 mb-2 p-2 border border-green-900 bg-black/50">
                                <button id="pickerABtn">Select A...</button>
                                <button id="btnSwapAB">Swap</button>
                                <button id="pickerBBtn">Select B...</button>
                            </div>
                            <div id="diffPretty" class="flex-1 overflow-y-auto"></div>
                            <pre id="diffRaw" class="hidden flex-1 overflow-y-auto"></pre>
                            <div id="sbsContainer" class="hidden flex-1 flex overflow-hidden">
                                <div id="sbsPaneA" class="flex-1 overflow-y-auto p-2 border-r border-green-900"></div>
                                <div id="sbsDivider" class="w-1 bg-green-900 cursor-col-resize hover:bg-green-500"></div>
                                <div id="sbsPaneB" class="flex-1 overflow-y-auto p-2"></div>
                            </div>
                        </div>

                        <div id="docMetricsView" class="h-full overflow-y-auto hidden">
                            <div class="grid grid-cols-4 gap-4 mb-4">
                                <div class="tui-panel p-4 text-center"><div class="text-xs text-green-800">Tokens</div><div id="mTokens" class="text-xl">-</div></div>
                                <div class="tui-panel p-4 text-center"><div class="text-xs text-green-800">Levenshtein</div><div id="mLev" class="text-xl">-</div></div>
                                <div class="tui-panel p-4 text-center"><div class="text-xs text-green-800">Hunks</div><div id="mHunks" class="text-xl">-</div></div>
                                <div class="tui-panel p-4 text-center"><div class="text-xs text-green-800">Bytes</div><div id="mBytes" class="text-xl">-</div></div>
                            </div>
                            <button id="btnComputeAll" class="w-full py-2 mb-2">Compute All Metrics</button>
                            <div id="computeProgress" class="text-xs text-green-600"></div>
                        </div>
                        
                        <div id="docSectionsView" class="h-full overflow-y-auto hidden">
                            <input id="sectionFilter" type="text" placeholder="Filter sections..." class="w-full mb-2">
                            <div id="sectionTableWrap">
                                <table id="sectionTable" class="w-full text-xs text-left">
                                    <thead class="border-b border-green-900"><tr><th class="p-2">Section</th><th class="p-2 text-right">Add</th><th class="p-2 text-right">Del</th></tr></thead>
                                    <tbody id="sectionTableBody"></tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </main>
    </div>

    <!-- Timeline Dock -->
    <div id="dock" class="dock">
        <div class="max-w-[1200px] mx-auto">
            <div class="flex justify-between items-center mb-2">
                <div class="flex gap-2">
                    <button id="dockPrev">Prev</button>
                    <button id="dockPlayPause">Play</button>
                    <button id="dockNext">Next</button>
                </div>
                <div id="dockTitle" class="text-xs text-green-500 font-mono"></div>
                <div class="text-xs text-green-800"><span id="dockLeftLabel"></span> - <span id="dockRightLabel"></span></div>
            </div>
            <canvas id="dockCanvas" class="dock-canvas mb-1"></canvas>
            <input id="dockSlider" type="range" class="w-full h-1 bg-green-900 appearance-none cursor-pointer" />
            <canvas id="dockHeatStripe" class="dock-canvas mt-1 h-2"></canvas>
        </div>
    </div>

    <!-- Hidden / Overlays -->
    <div id="searchPaletteOverlay" class="hidden fixed inset-0 bg-black/90 z-50 flex items-start justify-center pt-20">
        <div class="w-[600px] tui-panel p-0 bg-black">
            <input id="searchPaletteInput" type="text" class="w-full p-4 bg-transparent border-b border-green-900 outline-none text-lg font-mono text-green-400" placeholder="> Type command or search..." autofocus>
            <div id="searchPaletteResults" class="max-h-[400px] overflow-y-auto p-2"></div>
        </div>
    </div>
    
    <!-- Dependencies -->
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.10/dayjs.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.10/plugin/utc.js"></script>
    <script src="https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.9.0/build/highlight.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/markdown-it@14.1.0/dist/markdown-it.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/dompurify@3.1.0/dist/purify.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/diff2html/bundles/js/diff2html.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/diff@7.0.0/dist/diff.min.js"></script>

    <!-- Logic -->
    <script type="module">
        // [INJECTED JS CONTENT]
    </script>
</body>
</html>
"""

final_html = new_html.replace("// [INJECTED JS CONTENT]", js_content)
write_file(OUTPUT_FILE, final_html)
print(f"Successfully generated {OUTPUT_FILE}")
