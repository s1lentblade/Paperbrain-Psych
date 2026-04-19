'use strict';

const { Plugin, ItemView, Modal, Notice, WorkspaceLeaf } = require('obsidian');

const VIEW_TYPE   = 'paperbrain-papers';
const API_BASE    = 'http://localhost:27182';
const DEBOUNCE_MS = 300;

// ── Helpers ────────────────────────────────────────────────────────────────

function debounce(fn, ms) {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

async function api(path) {
    try {
        const r = await fetch(API_BASE + path, { signal: AbortSignal.timeout(8000) });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return await r.json();
    } catch (_) {
        return null;
    }
}

function fmt(n) { return (n || 0).toLocaleString(); }

// ── Paper Detail Modal ─────────────────────────────────────────────────────

class PaperModal extends Modal {
    constructor(app, paper) {
        super(app);
        this.paper = paper;
        this.modalEl.addClass('paperbrain-modal');
    }

    onOpen() {
        const { contentEl: el } = this;
        const p = this.paper;

        el.createEl('h2', { text: p.title || 'Untitled', cls: 'pb-paper-title' });

        // Meta row
        const meta = el.createDiv('pb-meta');
        meta.createSpan({ text: String(p.year || '?'), cls: 'pb-year' });
        meta.createSpan({ text: ' · ' });
        meta.createSpan({ text: `${fmt(p.cited_by_count)} citations`, cls: 'pb-citations' });
        if (p.type) {
            meta.createSpan({ text: ' · ' });
            meta.createSpan({ text: p.type, cls: 'pb-type' });
        }

        // Authors
        if (p.authors?.length) {
            el.createDiv({ text: p.authors.map(a => a.name).join(', '), cls: 'pb-authors' });
        }

        // DOI
        if (p.doi) {
            const doiDiv = el.createDiv('pb-doi');
            const a = doiDiv.createEl('a', { text: p.doi });
            a.href = p.doi.startsWith('http') ? p.doi : `https://doi.org/${p.doi}`;
            a.setAttr('target', '_blank');
            a.setAttr('rel', 'noopener noreferrer');
        }

        // Topics
        if (p.topics?.length) {
            const td = el.createDiv('pb-topics');
            td.createSpan({ text: 'Topics  ', cls: 'pb-label' });
            p.topics.forEach((t, i) => {
                if (i > 0) td.createSpan({ text: '  ·  ' });
                td.createSpan({
                    text: t.topic_name,
                    cls: t.is_primary ? 'pb-topic-primary' : 'pb-topic-secondary',
                });
            });
        }

        // Abstract
        if (p.abstract) {
            el.createEl('hr');
            const abs = el.createDiv('pb-abstract');
            abs.createEl('p', { text: p.abstract });
        }

        // Keywords
        if (p.keywords?.length) {
            const kd = el.createDiv('pb-keywords');
            kd.createSpan({ text: 'Keywords  ', cls: 'pb-label' });
            kd.createSpan({ text: p.keywords.join('  ·  ') });
        }
    }

    onClose() { this.contentEl.empty(); }
}

async function openPaper(app, paperId) {
    const data = await api(`/paper?id=${encodeURIComponent(paperId)}`);
    if (data && !data.error) {
        new PaperModal(app, data).open();
    } else {
        new Notice('Could not load paper. Is the server running?');
    }
}

// ── Search Modal ───────────────────────────────────────────────────────────

class SearchModal extends Modal {
    constructor(app) {
        super(app);
        this.modalEl.addClass('paperbrain-search');
    }

    onOpen() {
        const { contentEl: el } = this;
        el.createEl('h2', { text: 'Search PaperBrain' });

        // Input
        const input = el.createEl('input', {
            cls: 'pb-search-input',
            attr: { type: 'text', placeholder: 'Search 6.6M psychology papers...' },
        });

        // Filters
        const filters = el.createDiv('pb-filters');
        filters.createSpan({ text: 'Year: ' });
        const yMin = filters.createEl('input', { cls: 'pb-year-input', attr: { type: 'number', placeholder: '1900' } });
        filters.createSpan({ text: ' – ' });
        const yMax = filters.createEl('input', { cls: 'pb-year-input', attr: { type: 'number', placeholder: '2024' } });

        const results = el.createDiv('pb-results');

        const doSearch = debounce(async () => {
            const q = input.value.trim();
            results.empty();
            if (q.length < 2) return;

            results.createEl('p', { text: 'Searching…', cls: 'pb-hint' });

            let url = `/search?q=${encodeURIComponent(q)}&limit=20`;
            if (yMin.value) url += `&year_min=${yMin.value}`;
            if (yMax.value) url += `&year_max=${yMax.value}`;

            const data = await api(url);
            results.empty();

            if (!data) {
                results.createEl('p', {
                    text: 'Server not running — start it with: python scripts/db_server.py --start',
                    cls: 'pb-error',
                });
                return;
            }

            if (data.results.length === 0) {
                results.createEl('p', { text: 'No results found.', cls: 'pb-hint' });
                return;
            }

            results.createEl('p', {
                text: `${fmt(data.total_found)} results`,
                cls: 'pb-count',
            });

            data.results.forEach(p => {
                const item = results.createDiv('pb-result-item');
                item.createDiv({ text: p.title || 'Untitled', cls: 'pb-result-title' });

                const metaEl = item.createDiv('pb-result-meta');
                metaEl.createSpan({ text: String(p.year || '?'), cls: 'pb-year' });
                metaEl.createSpan({ text: `  ·  ${fmt(p.cited_by_count)} cit.` });
                if (p.primary_topic) {
                    metaEl.createSpan({ text: `  ·  ${p.primary_topic}`, cls: 'pb-topic-tag' });
                }
                if (p.abstract) {
                    item.createDiv({ text: p.abstract.slice(0, 200) + '…', cls: 'pb-snippet' });
                }

                item.addEventListener('click', () => openPaper(this.app, p.id));
            });
        }, DEBOUNCE_MS);

        input.addEventListener('input', doSearch);
        yMin.addEventListener('change', doSearch);
        yMax.addEventListener('change', doSearch);
        setTimeout(() => input.focus(), 50);
    }

    onClose() { this.contentEl.empty(); }
}

// ── Sidebar View ───────────────────────────────────────────────────────────

class PaperBrainView extends ItemView {
    constructor(leaf, plugin) {
        super(leaf);
        this.plugin       = plugin;   // access to graphMode + _toggleGraphMode
        this.topic        = null;
        this.sort         = 'citations';
        this.yearMin      = '';
        this.yearMax      = '';
        this.offset       = 0;
        this.limit        = 25;
        this.papers       = [];
        this.total        = 0;
        this.bridges      = null;
        this.showBridges  = false;
        this.loading      = false;
        this.searchQuery  = '';
        this.searchTotal  = 0;
        this.taxonomy     = null;  // cached after first fetch
        this.filterOpen   = false;
        this.filterSub    = '';    // which subfield is expanded in the tree
    }

    getViewType()    { return VIEW_TYPE; }
    getDisplayText() { return 'PaperBrain'; }
    getIcon()        { return 'book-open'; }

    async onOpen() {
        this.contentEl.addClass('paperbrain-sidebar-content');
        this._render();
    }

    // Called by plugin when the active topic note changes
    async setTopic(name) {
        if (name === this.topic) return;
        this.topic       = name;
        this.offset      = 0;
        this.papers      = [];
        this.total       = 0;
        this.bridges     = null;
        this.showBridges = false;
        await this._load();
        this._render();
    }

    async _load() {
        this.loading = true;

        if (this.searchQuery.trim().length >= 2) {
            // ── Search mode ──────────────────────────────────────────
            let url = `/search?q=${encodeURIComponent(this.searchQuery)}&limit=${this.limit}`;
            if (this.yearMin) url += `&year_min=${this.yearMin}`;
            if (this.yearMax) url += `&year_max=${this.yearMax}`;
            // offset not supported for search — always fresh
            const data = await api(url);
            this.loading = false;
            if (data) {
                this.papers      = data.results;
                this.searchTotal = data.total_found;
                this.total       = data.total_found;
            }
        } else if (this.topic) {
            // ── Topic mode ───────────────────────────────────────────
            let url = `/topic?name=${encodeURIComponent(this.topic)}`
                    + `&sort=${this.sort}&limit=${this.limit}&offset=${this.offset}`;
            if (this.yearMin) url += `&year_min=${this.yearMin}`;
            if (this.yearMax) url += `&year_max=${this.yearMax}`;
            const data = await api(url);
            this.loading = false;
            if (data) {
                this.papers = this.offset === 0 ? data.results : [...this.papers, ...data.results];
                this.total  = data.total;
            }
        } else {
            this.loading = false;
        }
    }

    _render() {
        const root = this.contentEl;
        root.empty();

        const inSearchMode = this.searchQuery.trim().length >= 2;

        // ── Graph mode toggle (always visible) ───────────────────────
        const isBrain = this.plugin?.graphMode === 'brain';
        const graphRow = root.createDiv('pb-graph-row');
        const graphBtn = graphRow.createEl('button', {
            text: isBrain ? '◉ Brain View' : '◎ Topic Map',
            cls:  'pb-graph-btn' + (isBrain ? ' pb-graph-brain' : ' pb-graph-topic'),
        });
        graphBtn.addEventListener('click', async () => {
            graphBtn.disabled = true;
            graphBtn.setText('Switching…');
            await this.plugin._toggleGraphMode();
            graphBtn.disabled = false;
        });

        // ── Search bar (always visible) ──────────────────────────────
        const searchRow = root.createDiv('pb-sidebar-search-row');
        const searchInput = searchRow.createEl('input', {
            cls:  'pb-sidebar-search',
            attr: { type: 'text', placeholder: 'Search all papers…' },
        });
        searchInput.value = this.searchQuery;

        // Clear button — only shown when there's a query
        if (inSearchMode) {
            const clearBtn = searchRow.createEl('button', { text: '×', cls: 'pb-search-clear' });
            clearBtn.addEventListener('click', async () => {
                this.searchQuery = '';
                this.offset      = 0;
                this.papers      = [];
                await this._load();
                this._render();
            });
        }

        // Debounced search handler
        const doSearch = debounce(async (val) => {
            this.searchQuery = val;
            this.offset      = 0;
            this.papers      = [];
            await this._load();
            this._render();
        }, DEBOUNCE_MS);

        searchInput.addEventListener('input', e => doSearch(e.target.value));

        // ── Sort + Filter button (always visible, not in search mode) ──
        if (!inSearchMode) {
            const ctls = root.createDiv('pb-controls');
            [
                ['citations', 'Top cited'],
                ['year_desc', 'Newest'],
                ['year_asc',  'Oldest'],
            ].forEach(([val, label]) => {
                const btn = ctls.createEl('button', { text: label, cls: 'pb-sort-btn' });
                if (val === this.sort) btn.addClass('pb-active');
                btn.addEventListener('click', async () => {
                    if (val === this.sort) return;
                    this.sort       = val;
                    this.offset     = 0;
                    this.papers     = [];
                    this.filterOpen = false;
                    await this._load();
                    this._render();
                });
            });

            // Filter button — opens immediately, fetches taxonomy in background
            const filterBtn = ctls.createEl('button', {
                text: this.filterOpen ? '⊞ Filter ▲' : '⊞ Filter ▼',
                cls: 'pb-sort-btn pb-filter-btn' + (this.filterOpen ? ' pb-active' : ''),
            });
            filterBtn.addEventListener('click', () => {
                this.filterOpen = !this.filterOpen;
                this._render();                         // immediate: show panel or hide it
                if (this.filterOpen && !this.taxonomy) {
                    this._fetchTaxonomy();              // async fill-in
                }
            });
        }

        // ── Filter panel (renders before paper list, no topic needed) ─
        if (this.filterOpen && !inSearchMode) {
            try {
                this._renderFilterPanel(root);
            } catch (e) {
                new Notice('PaperBrain filter error: ' + e.message);
                root.createEl('p', { text: 'Filter error: ' + e.message, cls: 'pb-error' });
            }
        }

        // ── Guard: nothing to show below without a topic or search ────
        if (!this.topic && !inSearchMode) {
            if (!this.filterOpen) {
                root.createEl('p', { text: 'Open a Topic note, search above, or use Filter.', cls: 'pb-hint' });
            }
            return;
        }

        if (this.loading) {
            root.createEl('p', { text: 'Loading…', cls: 'pb-hint' });
            return;
        }

        // ── Header ───────────────────────────────────────────────────
        const hdr = root.createDiv('pb-header');
        if (inSearchMode) {
            hdr.createEl('h3', { text: 'Search results', cls: 'pb-topic-name' });
            hdr.createSpan({ text: `${fmt(this.searchTotal)} found`, cls: 'pb-count' });
        } else {
            hdr.createEl('h3', { text: this.topic, cls: 'pb-topic-name' });
            hdr.createSpan({ text: `${fmt(this.total)} papers`, cls: 'pb-count' });
        }

        // Year filter (both modes)
        const yr = root.createDiv('pb-year-row');
        yr.createSpan({ text: 'Year: ' });
        const yMin = yr.createEl('input', { cls: 'pb-year-input', attr: { type: 'number', placeholder: 'from' } });
        yMin.value = this.yearMin;
        yr.createSpan({ text: ' – ' });
        const yMax = yr.createEl('input', { cls: 'pb-year-input', attr: { type: 'number', placeholder: 'to' } });
        yMax.value = this.yearMax;

        const applyYear = debounce(async () => {
            this.yearMin = yMin.value;
            this.yearMax = yMax.value;
            this.offset  = 0;
            this.papers  = [];
            await this._load();
            this._render();
        }, 400);
        yMin.addEventListener('input', applyYear);
        yMax.addEventListener('input', applyYear);

        // ── Paper list ───────────────────────────────────────────────
        const list = root.createDiv('pb-paper-list');

        if (this.papers.length === 0) {
            list.createEl('p', {
                text: inSearchMode ? 'No results found.' : 'No papers found.',
                cls: 'pb-hint',
            });
        } else {
            this.papers.forEach(p => this._paperRow(list, p, false, inSearchMode));
        }

        // Load more (topic mode only — search always returns top-N)
        if (!inSearchMode && this.papers.length < this.total) {
            const remaining = this.total - this.papers.length;
            const btn = root.createEl('button', {
                text: `Load more (${fmt(remaining)} remaining)`,
                cls: 'pb-load-more',
            });
            btn.addEventListener('click', async () => {
                this.offset += this.limit;
                await this._load();
                this._render();
            });
        }

        // ── Bridge papers (topic mode only) ──────────────────────────
        if (!inSearchMode && this.topic) {
            const bHdr   = root.createDiv('pb-bridges-header');
            const bToggle = bHdr.createEl('button', {
                text: `${this.showBridges ? '▾' : '▸'}  Bridge Papers`,
                cls:  'pb-bridges-toggle',
            });
            bToggle.addEventListener('click', async () => {
                this.showBridges = !this.showBridges;
                if (this.showBridges && this.bridges === null) {
                    const data = await api(`/bridges?name=${encodeURIComponent(this.topic)}&limit=10`);
                    this.bridges = data?.results ?? [];
                }
                this._render();
            });

            if (this.showBridges && this.bridges) {
                const bList = root.createDiv('pb-bridge-list');
                if (this.bridges.length === 0) {
                    bList.createEl('p', { text: 'No bridge papers found.', cls: 'pb-hint' });
                } else {
                    this.bridges.forEach(p => this._paperRow(bList, p, true));
                }
            }
        }
    }

    async _fetchTaxonomy() {
        const data = await api('/taxonomy');
        if (data?.subfields) {
            this.taxonomy = data.subfields;
            if (this.filterOpen) this._render();
        } else {
            new Notice('PaperBrain: could not reach server — run: python scripts/db_server.py --start');
            this.filterOpen = false;
            this._render();
        }
    }

    _renderFilterPanel(root) {
        const panel = root.createDiv('pb-filter-panel');

        if (!this.taxonomy) {
            panel.createEl('p', { text: 'Loading…', cls: 'pb-hint' });
            return;
        }

        const subfields = Object.keys(this.taxonomy).sort();

        // ── Subfield tree ─────────────────────────────────────────────
        const tree = panel.createDiv('pb-filter-tree');

        // "All" row — resets subfield filter
        const allRow = tree.createDiv('pb-filter-sf-row' + (!this.filterSub ? ' pb-filter-sf-active' : ''));
        allRow.createSpan({ text: (!this.filterSub ? '▼' : '▷') + '  All subfields', cls: 'pb-filter-sf-label' });
        allRow.addEventListener('click', () => {
            this.filterSub = '';
            this._render();
        });

        subfields.forEach(sf => {
            const isOpen = this.filterSub === sf;
            const sfRow  = tree.createDiv('pb-filter-sf-row' + (isOpen ? ' pb-filter-sf-active' : ''));
            sfRow.createSpan({
                text: (isOpen ? '▽' : '▷') + '  ' + sf,
                cls: 'pb-filter-sf-label',
            });
            sfRow.addEventListener('click', () => {
                this.filterSub = isOpen ? '' : sf;
                this._render();
            });
        });

        // ── Topics: only shown when a subfield is selected ───────────
        if (!this.filterSub) {
            panel.createEl('p', {
                text: 'Select a subfield above to browse its topics.',
                cls: 'pb-hint',
            });
            return;
        }

        const topicSource = this.taxonomy[this.filterSub] || [];
        if (topicSource.length === 0) return;

        panel.createEl('p', {
            text: this.filterSub,
            cls: 'pb-filter-topics-header',
        });

        const topicList = panel.createDiv('pb-filter-topics');
        topicSource.forEach(({ topic, count }) => {
            const row = topicList.createDiv('pb-filter-topic-row');
            if (topic === this.topic) row.addClass('pb-filter-topic-active');
            row.createSpan({ text: topic, cls: 'pb-filter-topic-name' });
            row.createSpan({ text: fmt(count), cls: 'pb-filter-topic-count' });
            row.addEventListener('click', async () => {
                this.topic       = topic;
                this.filterOpen  = false;
                this.filterSub   = '';
                this.offset      = 0;
                this.papers      = [];
                this.bridges     = null;
                this.showBridges = false;
                await this._load();
                this._render();
            });
        });
    }

    _paperRow(container, paper, isBridge = false, showTopic = false) {
        const row = container.createDiv('pb-paper-row' + (isBridge ? ' pb-bridge' : ''));

        row.createDiv({ text: paper.title || 'Untitled', cls: 'pb-paper-title' });

        const meta = row.createDiv('pb-paper-meta');
        meta.createSpan({ text: String(paper.year || '?'), cls: 'pb-year' });
        meta.createSpan({ text: `  ·  ${fmt(paper.cited_by_count)} cit.` });
        if (isBridge && paper.cross_topic_count) {
            meta.createSpan({ text: `  ·  ${paper.cross_topic_count} topics`, cls: 'pb-bridge-count' });
        }
        if (showTopic && paper.primary_topic) {
            meta.createSpan({ text: `  ·  ${paper.primary_topic}`, cls: 'pb-topic-tag' });
        }

        row.addEventListener('click', () => openPaper(this.app, paper.id));
    }
}

// ── Plugin ─────────────────────────────────────────────────────────────────

class PaperBrainPlugin extends Plugin {
    async onload() {
        this.graphMode = 'topic';   // will be synced from graph.json below
        this.registerView(VIEW_TYPE, leaf => new PaperBrainView(leaf, this));

        this.addRibbonIcon('book-open', 'PaperBrain — open paper browser', () => this._openSidebar());

        this.addCommand({
            id:       'search-papers',
            name:     'Search Papers',
            hotkeys:  [{ modifiers: ['Ctrl', 'Shift'], key: 'F' }],
            callback: () => new SearchModal(this.app).open(),
        });

        this.addCommand({
            id:       'open-browser',
            name:     'Open Paper Browser',
            callback: () => this._openSidebar(),
        });

        // Update sidebar whenever the active note changes
        this.registerEvent(
            this.app.workspace.on('file-open', f => this._onFileOpen(f))
        );

        this.addCommand({
            id:       'toggle-graph-mode',
            name:     'Toggle Graph: Topic Map ↔ Brain View',
            callback: () => this._toggleGraphMode(),
        });

        this.addRibbonIcon('git-fork', 'Toggle graph: Topic Map ↔ Brain View',
            () => this._toggleGraphMode());

        // Auto-open sidebar and ensure server is running once layout is ready
        this.app.workspace.onLayoutReady(async () => {
            await this._syncGraphMode();
            await this._ensureServer();
            await this._openSidebar();
            this._onFileOpen(this.app.workspace.getActiveFile());
        });
    }

    async onunload() {
        this.app.workspace.detachLeavesOfType(VIEW_TYPE);
    }

    async _ensureServer() {
        // Already running?
        const health = await api('/health');
        if (health?.status === 'ok') return;

        new Notice('PaperBrain: starting server…');

        try {
            const { spawn } = require('child_process');
            const path      = require('path');

            // Vault is at  .../full-psych/full psychology breakdown/
            // Script is at .../full-psych/scripts/db_server.py
            const vaultPath  = this.app.vault.adapter.basePath;
            const scriptPath = path.join(vaultPath, '..', 'scripts', 'db_server.py');

            // Use PowerShell to launch detached so it survives Obsidian restarts
            const proc = spawn(
                'powershell',
                ['-WindowStyle', 'Hidden', '-Command',
                 `Start-Process python -ArgumentList '"${scriptPath}"' -WindowStyle Hidden`],
                { detached: true, stdio: 'ignore', windowsHide: true }
            );
            proc.unref();
        } catch (e) {
            new Notice(`PaperBrain: could not spawn server — ${e.message}`);
            return;
        }

        // Poll /health until ready (max 20 s)
        for (let i = 0; i < 20; i++) {
            await new Promise(r => setTimeout(r, 1000));
            const h = await api('/health');
            if (h?.status === 'ok') {
                new Notice('PaperBrain: server ready');
                return;
            }
        }

        new Notice('PaperBrain: server did not start — run: python scripts/db_server.py --start');
    }

    async _syncGraphMode() {
        try {
            const raw = await this.app.vault.adapter.read(
                `${this.app.vault.configDir}/graph.json`
            );
            const cfg = JSON.parse(raw);
            this.graphMode = cfg.search?.includes('Papers') ? 'topic' : 'brain';
        } catch (_) {
            this.graphMode = 'topic';
        }
    }

    async _toggleGraphMode() {
        const newMode = this.graphMode === 'topic' ? 'brain' : 'topic';
        const cfgPath = `${this.app.vault.configDir}/graph.json`;

        try {
            const raw = await this.app.vault.adapter.read(cfgPath);
            const cfg = JSON.parse(raw);
            cfg.search = newMode === 'topic' ? '-path:"Papers"' : '';
            await this.app.vault.adapter.write(cfgPath, JSON.stringify(cfg, null, 2));
            this.graphMode = newMode;
        } catch (e) {
            new Notice('PaperBrain: could not update graph.json — ' + e.message);
            return;
        }

        // Refresh graph view if it's open
        const leaves = this.app.workspace.getLeavesOfType('graph');
        if (leaves.length > 0) {
            leaves.forEach(l => l.detach());
            await new Promise(r => setTimeout(r, 150));
            this.app.commands.executeCommandById('graph:open');
        }

        new Notice(newMode === 'brain'
            ? 'Graph → Brain View  (papers visible)'
            : 'Graph → Topic Map  (papers hidden)');

        // Re-render sidebar button
        const view = this._getView();
        if (view) view._render();
    }

    _getView() {
        const leaves = this.app.workspace.getLeavesOfType(VIEW_TYPE);
        return leaves.length > 0 ? leaves[0].view : null;
    }

    async _openSidebar() {
        const existing = this.app.workspace.getLeavesOfType(VIEW_TYPE);
        if (existing.length > 0) {
            this.app.workspace.revealLeaf(existing[0]);
            return;
        }
        const leaf = this.app.workspace.getRightLeaf(false);
        await leaf.setViewState({ type: VIEW_TYPE, active: true });
        this.app.workspace.revealLeaf(leaf);
    }

    _onFileOpen(file) {
        if (!file) return;
        const view = this._getView();
        if (!view) return;

        // Detect topic notes by folder
        if (file.path.startsWith('Topics/')) {
            view.setTopic(file.basename);
        }
    }
}

module.exports = PaperBrainPlugin;
