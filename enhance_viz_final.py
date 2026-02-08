
import re

INPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"
OUTPUT_FILE = "visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

original_html = read_file(INPUT_FILE)

# 1. Enhanced CSS (Preserving critical structural flex rules)
# We add the "world class" visual styles but ensure .main-container and .dock structure remains compatible
new_css = """
    <style>
        :root {
            --bg-color: #020202;
            --fg-color: #33ff00;
            --dim-color: #004400;
            --accent-color: #bd00ff;
            --panel-bg: rgba(10, 10, 10, 0.85);
            --border-color: #33ff00;
            
            /* Category Colors */
            --c1: #39ff14; --c2: #adff2f; --c3: #00ff00; --c4: #ff003c; --c5: #94a3b8;
            --c6: #d946ef; --c7: #00f2ff; --c8: #bf00ff; --c9: #00ffcc; --c10: #475569;
        }

        body {
            background-color: var(--bg-color);
            background-image: 
                radial-gradient(circle at 50% 0%, #001100 0%, transparent 60%),
                linear-gradient(0deg, rgba(0,0,0,0.2) 50%, transparent 50%);
            background-size: 100% 100%, 100% 4px;
            color: var(--fg-color);
            font-family: 'JetBrains Mono', monospace;
            margin: 0;
            height: 100vh;
            overflow: hidden;
            display: flex;
            flex-direction: column;
            text-shadow: 0 0 2px rgba(51, 255, 0, 0.3);
        }

        /* Scanline Overlay */
        body::after {
            content: "";
            position: fixed;
            inset: 0;
            background: linear-gradient(rgba(18, 16, 16, 0) 50%, rgba(0, 0, 0, 0.1) 50%), 
                        linear-gradient(90deg, rgba(255, 0, 0, 0.03), rgba(0, 255, 0, 0.01), rgba(0, 0, 255, 0.03));
            background-size: 100% 3px, 3px 100%;
            pointer-events: none;
            z-index: 9999;
            opacity: 0.4;
            mix-blend-mode: overlay;
        }

        /* Layout Structure */
        .main-container { flex: 1; display: flex; overflow: hidden; position: relative; }
        .sidebar { 
            width: 320px; 
            border-right: 1px solid rgba(51, 255, 0, 0.2);
            background: rgba(5, 5, 5, 0.9);
            box-shadow: 5px 0 20px rgba(0,0,0,0.5);
            display: flex; flex-direction: column; flex-shrink: 0; z-index: 20; 
            backdrop-filter: blur(12px);
        }
        .content-area { flex: 1; display: flex; flex-direction: column; overflow: hidden; background: transparent; }
        
        .pane-header { 
            padding: 0.6rem 1rem;
            background: linear-gradient(90deg, rgba(0,20,0,0.8), rgba(0,0,0,0.8));
            border-bottom: 1px solid var(--dim-color);
            font-size: 0.7rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            font-weight: 700;
            color: var(--fg-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            height: 44px;
            flex-shrink: 0;
            backdrop-filter: blur(5px);
        }
        .pane-scroll { flex: 1; overflow-y: auto; padding: 1rem; position: relative; }

        /* Commit List */
        .commit-item { 
            border: 1px solid transparent;
            border-left: 2px solid var(--dim-color);
            padding: 0.75rem;
            margin-bottom: 4px;
            background: rgba(255,255,255,0.02);
            cursor: pointer;
            transition: all 0.15s ease-out;
            position: relative;
            overflow: hidden;
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

        /* Controls */
        input[type="text"], input[type="search"], select {
            background: rgba(0,0,0,0.5);
            border: 1px solid var(--dim-color);
            color: var(--fg-color);
            border-radius: 4px;
            padding: 6px 8px;
            font-family: inherit;
            width: 100%;
            transition: border-color 0.2s;
        }
        input:focus, select:focus { outline: none; border-color: var(--fg-color); box-shadow: 0 0 5px var(--dim-color); }

        button {
            border: 1px solid var(--dim-color);
            color: var(--fg-color);
            background: rgba(0,0,0,0.3);
            border-radius: 4px;
            padding: 4px 10px;
            font-size: 0.7rem;
            font-family: inherit;
            cursor: pointer;
            transition: all 0.2s;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        button:hover { border-color: var(--fg-color); box-shadow: 0 0 8px var(--dim-color); text-shadow: 0 0 5px var(--fg-color); }
        button.active { background: var(--fg-color); color: #000; font-weight: bold; box-shadow: 0 0 10px var(--dim-color); }

        /* Floating Dock */
        .dock {
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            width: 90%;
            max-width: 1000px;
            height: auto;
            background: rgba(10, 10, 10, 0.9);
            border: 1px solid var(--dim-color);
            border-radius: 12px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.5), 0 0 20px rgba(51, 255, 0, 0.05);
            padding: 12px 20px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            z-index: 1000;
            backdrop-filter: blur(15px);
            transition: transform 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);
        }
        .dock:hover { border-color: var(--fg-color); box-shadow: 0 15px 40px rgba(0,0,0,0.6), 0 0 25px rgba(51, 255, 0, 0.1); }
        .dock-controls { display: flex; justify-content: space-between; align-items: center; }
        .dock-canvas { width: 100%; height: 30px; display: block; border-radius: 4px; opacity: 0.8; transition: opacity 0.2s; }
        .dock-canvas:hover { opacity: 1; }
        input[type="range"] { width: 100%; accent-color: var(--fg-color); height: 4px; background: var(--dim-color); border: none; border-radius: 2px; }

        /* Loading Overlay */
        #loadingOverlay {
            background: rgba(0,0,0,0.9);
            backdrop-filter: blur(10px);
            z-index: 2000;
        }
        .loader-spinner {
            width: 40px; height: 40px;
            border: 3px solid var(--dim-color);
            border-top-color: var(--fg-color);
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto 1rem auto;
        }
        @keyframes spin { 100% { transform: rotate(360deg); } }

        /* Panels & Charts */
        .tui-panel {
            background: var(--panel-bg);
            border: 1px solid rgba(51, 255, 0, 0.1);
            border-radius: 8px;
            transition: border-color 0.2s;
        }
        .tui-panel:hover { border-color: var(--dim-color); background: rgba(15, 15, 15, 0.9); }
        .chart-container { width: 100%; height: 100%; min-height: 200px; position: relative; }

        /* Markdown */
        .doc-view { padding-right: 1rem; max-width: 900px; margin: 0 auto; color: #ccc; font-size: 0.9rem; line-height: 1.6; }
        .doc-view h1, .doc-view h2, .doc-view h3 { color: var(--fg-color); border-bottom: 1px dashed var(--dim-color); padding-bottom: 0.5rem; margin-top: 2rem; font-family: 'Fraunces', serif; }
        .doc-view code { color: #bd00ff; background: #151515; padding: 2px 4px; border-radius: 4px; font-size: 0.85em; border: 1px solid #333; }
        .doc-view pre { background: #080808; border: 1px solid var(--dim-color); padding: 1rem; overflow-x: auto; margin: 1rem 0; border-radius: 6px; box-shadow: inset 0 0 10px rgba(0,0,0,0.5); }
        
        /* Header */
        header { 
            flex-shrink: 0; 
            border-bottom: 1px solid var(--dim-color); 
            padding: 0.5rem 1rem; 
            display: flex; justify-content: space-between; align-items: center; 
            background: rgba(0,0,0,0.8); 
            z-index: 50; height: 50px; 
            backdrop-filter: blur(10px); 
        }
        h1 { margin: 0; font-size: 1.1rem; text-transform: uppercase; letter-spacing: 0.1em; text-shadow: 0 0 10px rgba(51, 255, 0, 0.4); }

        /* Helpers */
        .hidden { display: none !important; }
        .text-dim { color: #666; }
        .flex { display: flex; }
        .flex-col { flex-direction: column; }
        .gap-2 { gap: 0.5rem; }
        .mb-4 { margin-bottom: 1rem; }
        .w-full { width: 100%; }
        
        /* Galaxy Brain Animation */
        @keyframes pulse-galaxy {
            0% { box-shadow: 0 0 5px var(--accent-color); }
            50% { box-shadow: 0 0 20px var(--accent-color); }
            100% { box-shadow: 0 0 5px var(--accent-color); }
        }
        .galaxy-active {
            animation: pulse-galaxy 2s infinite;
            border-color: var(--accent-color) !important;
            color: var(--accent-color) !important;
        }

        /* Scrollbar */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: var(--dim-color); border-radius: 3px; border: 1px solid #000; }
        ::-webkit-scrollbar-thumb:hover { background: var(--fg-color); box-shadow: 0 0 5px var(--fg-color); }
    </style>
"""

# Replace existing styles
new_html = re.sub(r'<style>.*?</style>', new_css, original_html, flags=re.DOTALL)

# 2. Enhanced Dock HTML (Floating style)
new_dock = """
<div class="dock">
    <div class="dock-controls">
        <div class="flex gap-3 items-center">
            <button id="dockPrev" class="text-lg" title="Previous (Left Arrow)">&#9664;</button>
            <button id="dockPlay" class="w-20 font-bold tracking-widest" title="Play/Pause (Space)">PLAY</button>
            <button id="dockNext" class="text-lg" title="Next (Right Arrow)">&#9654;</button>
            <div style="height: 16px; width: 1px; background: var(--dim-color); margin: 0 8px;"></div>
            <button id="dockLoop" class="text-[10px] py-1 px-2" title="Loop Playback">LOOP</button>
        </div>
        <div class="flex flex-col items-end">
            <div id="dockTitle" class="text-xs font-bold truncate" style="max-width: 400px; color: var(--fg-color);"></div>
            <div id="dockLabel" class="text-[10px] font-mono" style="color: var(--dim-color);"></div>
        </div>
    </div>
    <div style="position: relative;">
        <canvas id="dockCanvas" class="dock-canvas"></canvas>
    </div>
    <input id="dockSlider" type="range" min="0" max="100" value="0">
    <div id="dockHeatTooltip" class="hidden absolute -top-12 left-0 bg-black border border-green-500 p-2 text-xs z-[200] rounded shadow-lg pointer-events-none whitespace-nowrap"></div>
</div>
"""
new_html = re.sub(r'<div class="dock">.*?</div>\s*</div>', new_dock, new_html, flags=re.DOTALL)

# 3. Enhanced Loader
new_loader = """
            <div id="loadingOverlay" class="absolute inset-0 bg-black/90 flex flex-col items-center justify-center z-50 transition-opacity duration-500">
                <div class="loader-spinner"></div>
                <div class="text-xl mb-1 font-bold tracking-widest animate-pulse" style="color:var(--fg-color)">SYSTEM BOOT</div>
                <div class="text-xs text-dim font-mono">Initializing SQLite Neural Core...</div>
            </div>
"""
new_html = re.sub(r'<div id="loadingOverlay".*?</div>', new_loader, new_html, flags=re.DOTALL)

# 4. Inject Interaction JS (Loader fade, Galaxy Brain, Keyboard)
js_injections = """
    // --- Advanced UI Interactions ---
    function hideLoader() {
        const el = document.getElementById("loadingOverlay");
        if (el) {
            el.style.opacity = '0';
            setTimeout(() => el.classList.add("hidden"), 500);
        }
    }

    let galaxyMode = false;
    document.getElementById("btnGalaxy").addEventListener("click", () => {
        galaxyMode = !galaxyMode;
        const btn = document.getElementById("btnGalaxy");
        if (galaxyMode) {
            btn.classList.add("galaxy-active");
            btn.textContent = "GALAXY ACTIVE";
            document.documentElement.style.setProperty('--accent-color', '#ff00ff');
            document.documentElement.style.setProperty('--fg-color', '#00ffff');
            document.body.style.backgroundImage = "radial-gradient(circle at 50% 50%, #110011 0%, #000 70%)";
            document.getElementById("tabStats").click();
        } else {
            btn.classList.remove("galaxy-active");
            btn.textContent = "Galaxy Brain";
            document.documentElement.style.setProperty('--accent-color', '#bd00ff');
            document.documentElement.style.setProperty('--fg-color', '#33ff00');
            document.body.style.backgroundImage = "";
        }
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
new_html = new_html.replace('init();', js_injections + '
    init();')

# 5. Fix loader hide call
new_html = new_html.replace('document.getElementById("loadingOverlay").classList.add("hidden");', 'hideLoader();')

# 6. Apply Chart Styles
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
# Replace the old opts definition using regex to capture the whole block
new_html = re.sub(r'const opts = \{.*?\};', charts_styling, new_html, flags=re.DOTALL)

write_file(OUTPUT_FILE, new_html)
print(f"Enhanced {OUTPUT_FILE}")
