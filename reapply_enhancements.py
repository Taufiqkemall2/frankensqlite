
import re

INPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"
OUTPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

html = read_file(INPUT_FILE)

# 1. Remove redundant CSS link if present (it might have been removed by fix_viz_errors, but checking is safe)
html = re.sub(r'<link rel="stylesheet" href="frankentui.css"\s*/>', '', html)

# 2. Inject World-Class CSS Overrides (appending to existing style block)
# Note: We append to the existing style block which now contains the embedded frankentui.css
extra_css = """
        /* --- World-Class Overrides --- */
        
        /* Deep Glassmorphism & Background */
        body {
            background-color: #020202;
            background-image: 
                radial-gradient(circle at 50% 0%, #001100 0%, transparent 60%),
                linear-gradient(0deg, rgba(0,0,0,0.2) 50%, transparent 50%);
            background-size: 100% 100%, 100% 4px;
            text-shadow: 0 0 2px rgba(51, 255, 0, 0.3);
        }
        
        /* Refined Scanline */
        body::after {
            background: linear-gradient(rgba(18, 16, 16, 0) 50%, rgba(0, 0, 0, 0.1) 50%), 
                        linear-gradient(90deg, rgba(255, 0, 0, 0.03), rgba(0, 255, 0, 0.01), rgba(0, 0, 255, 0.03));
            background-size: 100% 3px, 3px 100%;
            opacity: 0.4;
            mix-blend-mode: overlay;
        }

        /* Floating Glass Dock */
        .dock {
            position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%);
            width: 90%; max-width: 900px; height: auto;
            background: rgba(10, 10, 10, 0.85);
            border: 1px solid rgba(51, 255, 0, 0.3);
            border-radius: 16px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.6), 0 0 20px rgba(51, 255, 0, 0.05);
            backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
            padding: 12px 20px;
            display: flex; flex-direction: column; gap: 8px;
            z-index: 1000;
            transition: all 0.3s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        .dock:hover {
            border-color: var(--fg-color);
            box-shadow: 0 15px 40px rgba(0,0,0,0.7), 0 0 30px rgba(51, 255, 0, 0.15);
            transform: translateX(-50%) translateY(-2px);
        }

        /* Glass Panels */
        .sidebar, .pane-header, .tui-panel {
            background: rgba(5, 5, 5, 0.85);
            backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
            border-color: rgba(51, 255, 0, 0.15);
        }
        .pane-header {
            background: linear-gradient(90deg, rgba(0,20,0,0.6), rgba(0,0,0,0.6));
            border-bottom: 1px solid rgba(51, 255, 0, 0.2);
        }

        /* Animated Commit Items */
        .commit-item {
            border-left-width: 2px;
            margin-bottom: 4px;
            background: rgba(255,255,255,0.015);
            transition: all 0.2s ease-out;
            border-radius: 0 4px 4px 0;
        }
        .commit-item:hover {
            background: rgba(51, 255, 0, 0.05);
            border-left-color: var(--fg-color);
            padding-left: 1rem;
        }
        .commit-item.selected {
            background: linear-gradient(90deg, rgba(51, 255, 0, 0.1), transparent);
            border-left: 3px solid var(--fg-color);
            box-shadow: 0 0 15px rgba(51, 255, 0, 0.05);
        }

        /* Refined Controls */
        button {
            border-radius: 6px;
            background: rgba(0,0,0,0.4);
            border-color: rgba(51, 255, 0, 0.3);
            letter-spacing: 0.05em;
            transition: all 0.2s;
        }
        button:hover {
            background: rgba(51, 255, 0, 0.1);
            border-color: var(--fg-color);
            box-shadow: 0 0 10px rgba(51, 255, 0, 0.2);
            text-shadow: 0 0 8px var(--fg-color);
        }
        button.active {
            background: var(--fg-color);
            color: #000;
            box-shadow: 0 0 15px rgba(51, 255, 0, 0.4);
        }

        /* Galaxy Brain Mode */
        @keyframes galaxy-pulse {
            0% { box-shadow: 0 0 5px #ff00ff; }
            50% { box-shadow: 0 0 25px #ff00ff, 0 0 10px #00ffff; }
            100% { box-shadow: 0 0 5px #ff00ff; }
        }
        .galaxy-mode {
            --fg-color: #00ffff !important;
            --dim-color: #330033 !important;
            --border-color: #ff00ff !important;
            --accent-color: #ff00ff !important;
        }
        .galaxy-mode body {
            background-image: radial-gradient(circle at 50% 50%, #1a001a 0%, #000 80%);
        }
        .galaxy-active-btn {
            animation: galaxy-pulse 2s infinite;
            background: transparent !important;
            color: #ff00ff !important;
            border-color: #ff00ff !important;
            font-weight: 800;
        }

        /* Smooth Loader */
        #loadingOverlay {
            transition: opacity 0.6s ease-out;
            backdrop-filter: blur(20px);
            background: rgba(0,0,0,0.9);
        }
        .loader-spinner {
            width: 50px; height: 50px;
            border: 2px solid rgba(51, 255, 0, 0.1);
            border-top-color: var(--fg-color);
            border-radius: 50%;
            animation: spin 0.8s cubic-bezier(0.4, 0, 0.2, 1) infinite;
            margin-bottom: 1rem;
            box-shadow: 0 0 15px rgba(51, 255, 0, 0.2);
        }
        @keyframes spin { 100% { transform: rotate(360deg); } }
"""
# Append to the end of the existing style block
html = html.replace('</style>', extra_css + '</style>')

# 3. Update Dock HTML
new_dock_html = """
<div class="dock">
    <div class="dock-controls">
        <div class="flex gap-3 items-center">
            <button id="dockPrev" class="text-lg px-3 hover:scale-110" title="Previous">&#9664;</button>
            <button id="dockPlay" class="w-24 font-bold tracking-widest border-2" title="Play/Pause">PLAY</button>
            <button id="dockNext" class="text-lg px-3 hover:scale-110" title="Next">&#9654;</button>
            <div style="width:1px; height:20px; background:var(--dim-color); margin:0 10px"></div>
            <div id="dockTitle" class="text-xs font-bold truncate text-white" style="max-width: 300px; text-shadow:0 0 10px rgba(255,255,255,0.3);"></div>
        </div>
        <div class="text-[10px] font-mono text-dim"><span id="dockLabel"></span></div>
    </div>
    <div class="relative group w-full">
        <canvas id="dockCanvas" class="dock-canvas h-8 w-full rounded opacity-70 group-hover:opacity-100 transition-opacity cursor-crosshair"></canvas>
        <div class="absolute inset-0 pointer-events-none" style="box-shadow: inset 0 0 20px rgba(0,0,0,0.8);"></div>
    </div>
    <input id="dockSlider" type="range" min="0" max="100" value="0" class="w-full">
    <div id="dockHeatTooltip" class="hidden absolute -top-12 left-0 bg-black border border-green-500 p-2 text-xs z-[200] rounded shadow-lg pointer-events-none whitespace-nowrap"></div>
</div>
"""
# The structure might be slightly different after my restoration script, so I will match loosely
html = re.sub(r'<div class="dock">.*?</div>\s*</div>', new_dock_html, html, flags=re.DOTALL)
# Fallback regex if the above is too strict or whitespace differs
if '<div class="dock">' in html and 'dockHeatTooltip' not in html:
    html = re.sub(r'<div class="dock">.*?</div>', new_dock_html, html, flags=re.DOTALL)

# 4. Inject Enhanced JS Logic
js_injections = """
    // --- Advanced UI Interactions ---
    function hideLoader() {
        const el = document.getElementById("loadingOverlay");
        if (el) {
            el.style.opacity = '0';
            setTimeout(() => el.classList.add("hidden"), 600);
        }
    }

    let galaxyMode = false;
    document.getElementById("btnGalaxy").addEventListener("click", () => {
        galaxyMode = !galaxyMode;
        const btn = document.getElementById("btnGalaxy");
        if (galaxyMode) {
            btn.classList.add("galaxy-active-btn");
            btn.textContent = "GALAXY ACTIVE";
            document.body.classList.add("galaxy-mode");
            document.getElementById("tabStats").click();
        } else {
            btn.classList.remove("galaxy-active-btn");
            btn.textContent = "Galaxy Brain";
            document.body.classList.remove("galaxy-mode");
        }
        render(); // Force redraw for chart colors
    });

    document.addEventListener("keydown", e => {
        if (e.target.tagName === "INPUT") return;
        switch(e.key) {
            case " ": e.preventDefault(); document.getElementById("dockPlay").click(); break;
            case "ArrowLeft": selectCommit(Math.max(0, STATE.idx - 1)); break;
            case "ArrowRight": selectCommit(Math.min(COMMITS.length - 1, STATE.idx + 1)); break;
            case "h": document.getElementById("tabTimeline").click(); break;
            case "s": document.getElementById("tabSpec").click(); break;
            case "d": document.getElementById("tabDiff").click(); break;
        }
    });
"""
# Inject before init()
html = html.replace('init();', js_injections + '
    init();')

# 5. Fix loader hide call (replace the basic classList.add with the animated function)
# The current file uses document.getElementById("loadingOverlay").classList.add("hidden");
html = html.replace('document.getElementById("loadingOverlay").classList.add("hidden");', 'hideLoader();')

# 6. Enhance Loader HTML
new_loader_html = """
            <div id="loadingOverlay" class="absolute inset-0 bg-black/90 flex flex-col items-center justify-center z-50">
                <div class="loader-spinner"></div>
                <div class="text-2xl mb-2 font-bold tracking-[0.2em] animate-pulse" style="color:var(--fg-color); text-shadow:0 0 15px var(--fg-color);">SYSTEM INITIALIZATION</div>
                <div class="text-xs text-dim font-mono">Loading Neural Core...</div>
            </div>
"""
# Use regex to find and replace the simple loader div
html = re.sub(r'<div id="loadingOverlay".*?</div>', new_loader_html, html, flags=re.DOTALL)

# 7. Apply Chart Styles (Neon)
charts_styling = """
        const opts = { 
            backgroundColor: 'transparent', 
            textStyle: { fontFamily: 'JetBrains Mono', color: '#004400' }, 
            grid: { top: 40, bottom: 30, left: 50, right: 20, borderColor: '#002200' }, 
            tooltip: { 
                trigger: 'item', 
                backgroundColor: 'rgba(0,0,0,0.9)', 
                borderColor: '#33ff00', 
                textStyle: { color: '#eee' },
                padding: [10, 15],
                extraCssText: 'box-shadow: 0 0 10px rgba(51,255,0,0.3); border-radius: 4px;'
            },
            xAxis: { axisLine: { lineStyle: { color: '#004400' } }, axisLabel: { color: '#006600' }, splitLine: { show: false } },
            yAxis: { axisLine: { lineStyle: { color: '#004400' } }, axisLabel: { color: '#006600' }, splitLine: { lineStyle: { color: '#002200', type: 'dashed' } } }
        };
"""
html = re.sub(r'const opts = \{.*?\};', charts_styling, html, flags=re.DOTALL)

write_file(OUTPUT_FILE, html)
print(f"Finalized UI/UX in {OUTPUT_FILE}")
