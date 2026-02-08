
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

# 1. Enhanced CSS for World-Class Aesthetic
# We will inject improved CSS variables, animations, and glassmorphism.
css_enhancements = """
    <style>
        :root {
            --bg-color: #020202;
            --fg-color: #33ff00;
            --dim-color: #004400;
            --accent-color: #bd00ff;
            --panel-bg: rgba(10, 10, 10, 0.85); /* Glassmorphic base */
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
            text-shadow: 0 0 2px rgba(51, 255, 0, 0.3); /* Subtle glow for text */
        }

        /* Improved Scanline */
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

        /* Glassmorphic Panels */
        .tui-panel, .pane-header, .sidebar, .dock {
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
        }

        /* Sidebar Styling */
        .sidebar {
            width: 320px;
            border-right: 1px solid rgba(51, 255, 0, 0.2);
            background: rgba(5, 5, 5, 0.9);
            box-shadow: 5px 0 20px rgba(0,0,0,0.5);
            z-index: 20;
        }

        /* Pane Header */
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
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        }

        /* Commit List Items */
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
            padding-left: 1rem; /* Slight shift on hover */
        }
        .commit-item.selected {
            background: linear-gradient(90deg, rgba(51, 255, 0, 0.1), transparent);
            border-left: 3px solid var(--fg-color);
            box-shadow: 0 0 15px rgba(51, 255, 0, 0.05);
        }
        .commit-item::before {
            content: "";
            position: absolute;
            top: 0; left: 0; bottom: 0; width: 0%;
            background: var(--fg-color);
            opacity: 0.1;
            transition: width 0.2s;
        }
        .commit-item.selected::before { width: 100%; opacity: 0.02; }

        /* Buttons & Inputs */
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
        button:hover {
            border-color: var(--fg-color);
            box-shadow: 0 0 8px var(--dim-color);
            text-shadow: 0 0 5px var(--fg-color);
        }
        button.active {
            background: var(--fg-color);
            color: #000;
            font-weight: bold;
            box-shadow: 0 0 10px var(--dim-color);
        }
        
        input[type="text"], input[type="search"], select {
            background: rgba(0,0,0,0.5);
            border: 1px solid var(--dim-color);
            color: var(--fg-color);
            border-radius: 4px;
            padding: 6px 8px;
            font-family: inherit;
            transition: border-color 0.2s;
        }
        input:focus, select:focus {
            outline: none;
            border-color: var(--fg-color);
            box-shadow: 0 0 5px var(--dim-color);
        }

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
        .dock:hover {
            border-color: var(--fg-color);
            box-shadow: 0 15px 40px rgba(0,0,0,0.6), 0 0 25px rgba(51, 255, 0, 0.1);
        }
        .dock-controls { display: flex; justify-content: space-between; align-items: center; }
        
        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { 
            background: var(--dim-color); 
            border-radius: 3px; 
            border: 1px solid #000;
        }
        ::-webkit-scrollbar-thumb:hover { background: var(--fg-color); box-shadow: 0 0 5px var(--fg-color); }

        /* Chart Containers */
        .chart-container {
            border-radius: 8px;
            border: 1px solid rgba(51, 255, 0, 0.1);
            background: rgba(0,0,0,0.2);
            transition: border-color 0.2s;
        }
        .chart-container:hover {
            border-color: var(--dim-color);
            background: rgba(0,0,0,0.4);
        }

        /* Loading Overlay */
        #loadingOverlay {
            background: rgba(0,0,0,0.85);
            backdrop-filter: blur(5px);
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

        /* Code & Diff Styling */
        .doc-view pre, #diffContent pre {
            background: #080808 !important;
            border: 1px solid var(--dim-color);
            border-radius: 6px;
            box-shadow: inset 0 0 10px rgba(0,0,0,0.5);
        }
        .d2h-file-header { background-color: #111 !important; border-bottom: 1px solid var(--dim-color) !important; }
        
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
    </style>
"""

# Replace the existing style block
new_html = re.sub(r'<style>.*?</style>', css_enhancements, original_html, flags=re.DOTALL)

# 2. Modify Dock HTML for "Floating" Look
# We need to remove the old dock HTML and insert the new structure if needed, 
# but the CSS handles the floating positioning. 
# We'll just tweak the inner structure for better spacing.
new_dock_html = """
<div class="dock">
    <div class="dock-controls">
        <div class="flex gap-3 items-center">
            <button id="dockPrev" class="text-lg" title="Previous Commit (Left Arrow)">&#9664;</button>
            <button id="dockPlay" class="w-20 font-bold tracking-widest" title="Play/Pause (Space)">PLAY</button>
            <button id="dockNext" class="text-lg" title="Next Commit (Right Arrow)">&#9654;</button>
            <div class="h-4 w-[1px] bg-green-900 mx-2"></div>
            <span class="text-[10px] text-green-700 uppercase tracking-wide">Speed</span>
            <select id="dockSpeed" class="w-16 h-6 py-0 text-[10px] border-none bg-green-900/20"><option value="1">1x</option><option value="4">4x</option><option value="10">10x</option></select>
            <button id="dockLoop" class="text-[10px] py-1 px-2" title="Loop Playback">LOOP</button>
        </div>
        <div class="flex flex-col items-end">
            <div id="dockTitle" class="text-xs text-green-400 font-bold truncate max-w-[400px]"></div>
            <div class="text-[10px] text-green-800 font-mono"><span id="dockLeftLabel"></span> &mdash; <span id="dockRightLabel"></span></div>
        </div>
    </div>
    <div class="relative group">
        <canvas id="dockCanvas" class="dock-canvas h-8 w-full rounded cursor-crosshair opacity-80 group-hover:opacity-100 transition-opacity"></canvas>
        <div class="absolute top-0 left-0 w-full h-full pointer-events-none" style="box-shadow: inset 0 0 10px #000;"></div>
    </div>
    <input id="dockSlider" type="range" min="0" max="100" value="0" class="w-full cursor-pointer">
    <canvas id="dockHeatStripe" class="dock-canvas mt-1 h-1.5 rounded opacity-60"></canvas>
    <div id="dockHeatTooltip" class="hidden absolute -top-12 left-0 bg-black border border-green-500 p-2 text-xs z-[200] rounded shadow-lg pointer-events-none whitespace-nowrap"></div>
</div>
"""

new_html = re.sub(r'<div class="dock">.*?</div>\s*</div>', new_dock_html, new_html, flags=re.DOTALL)

# 3. Enhance Loading Overlay with Spinner
new_loader = """
            <div id="loadingOverlay" class="absolute inset-0 bg-black/90 flex flex-col items-center justify-center z-50 transition-opacity duration-500">
                <div class="loader-spinner"></div>
                <div class="text-xl mb-1 font-bold tracking-widest text-green-500 animate-pulse">SYSTEM BOOT</div>
                <div class="text-xs text-dim font-mono">Initializing SQLite Neural Core...</div>
            </div>
"""
new_html = re.sub(r'<div id="loadingOverlay".*?</div>\s*</div>', new_loader, new_html, flags=re.DOTALL)

# 4. Inject Advanced JS Interactions
# We need to add logic for the new loader transition, dock tooltip improvements, and "Galaxy Brain" visual mode.
js_injections = """
    // --- Advanced UI Interactions ---
    
    // Smooth Loader Transition
    function hideLoader() {
        const el = document.getElementById("loadingOverlay");
        el.style.opacity = '0';
        setTimeout(() => el.classList.add("hidden"), 500);
    }

    // Replace original load completion
    // (We will hook this into the init function via regex replacement later)

    // Galaxy Brain Visual Mode
    let galaxyMode = false;
    document.getElementById("btnGalaxy").addEventListener("click", () => {
        galaxyMode = !galaxyMode;
        const btn = document.getElementById("btnGalaxy");
        const body = document.body;
        
        if (galaxyMode) {
            btn.classList.add("galaxy-active");
            btn.textContent = "GALAXY ACTIVE";
            document.documentElement.style.setProperty('--accent-color', '#ff00ff'); // Hot pink shift
            document.documentElement.style.setProperty('--fg-color', '#00ffff'); // Cyan shift
            body.style.backgroundImage = "radial-gradient(circle at 50% 50%, #110011 0%, #000 70%)";
            document.getElementById("tabStats").click(); // Jump to stats
        } else {
            btn.classList.remove("galaxy-active");
            btn.textContent = "Galaxy Brain";
            document.documentElement.style.setProperty('--accent-color', '#bd00ff');
            document.documentElement.style.setProperty('--fg-color', '#33ff00');
            body.style.backgroundImage = "";
        }
    });

    // Enhanced Keyboard Shortcuts
    document.addEventListener("keydown", e => {
        if (e.target.tagName === "INPUT") return; // Ignore inputs
        
        switch(e.key) {
            case " ":
                e.preventDefault();
                document.getElementById("dockPlay").click();
                break;
            case "ArrowLeft":
                selectCommit(Math.max(0, STATE.idx - 1));
                break;
            case "ArrowRight":
                selectCommit(Math.min(COMMITS.length - 1, STATE.idx + 1));
                break;
            case "h":
                document.getElementById("tabTimeline").click();
                break;
            case "s":
                document.getElementById("tabSpec").click();
                break;
            case "d":
                document.getElementById("tabDiff").click();
                break;
        }
    });

    // Custom Tooltip for Dock Canvas
    const dockCanvas = document.getElementById("dockCanvas");
    const dockTooltip = document.getElementById("dockHeatTooltip");
    
    dockCanvas.addEventListener("mousemove", e => {
        const rect = dockCanvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const idx = Math.floor((x / rect.width) * COMMITS.length);
        
        if (COMMITS[idx]) {
            dockTooltip.classList.remove("hidden");
            dockTooltip.style.left = (x + 10) + "px";
            dockTooltip.innerHTML = `<div class='font-bold'>#${idx} ${COMMITS[idx].short}</div><div>${COMMITS[idx].subject.slice(0, 40)}...</div>`;
        }
    });
    
    dockCanvas.addEventListener("mouseleave", () => {
        dockTooltip.classList.add("hidden");
    });
    
    dockCanvas.addEventListener("click", e => {
        const rect = dockCanvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const idx = Math.floor((x / rect.width) * COMMITS.length);
        selectCommit(idx);
    });

"""

# Inject the JS before the end of the script tag
new_html = new_html.replace('init();', js_injections + '
    init();')

# 5. Fix the Loading Overlay fade-out call
# Find where loadingOverlay is hidden and replace it with our smooth function
new_html = new_html.replace('document.getElementById("loadingOverlay").classList.add("hidden");', 'hideLoader();')

# 6. Refine ECharts Themes for Neon Look
# We'll update the renderCharts function to use improved styling
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
print(f"Successfully elevated UI/UX in {OUTPUT_FILE}")
