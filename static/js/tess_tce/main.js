(function () {
    const ui = {
        runBtn: document.getElementById("runBtn"),
        clearCacheBtn: document.getElementById("clearCacheBtn"),
        fetchTopDvBtn: document.getElementById("fetchTopDvBtn"),
        fetchTopVarBtn: document.getElementById("fetchTopVarBtn"),
        fetchTopStatusBtn: document.getElementById("fetchTopStatusBtn"),
        fetchSelectedVarBtn: document.getElementById("fetchSelectedVarBtn"),
        fetchSelectedDvBtn: document.getElementById("fetchSelectedDvBtn"),
        sectorInput: document.getElementById("sectorInput"),
        limitInput: document.getElementById("limitInput"),
        minMesInput: document.getElementById("minMesInput"),
        maxDepthInput: document.getElementById("maxDepthInput"),
        minDurationInput: document.getElementById("minDurationInput"),
        maxDurationInput: document.getElementById("maxDurationInput"),
        sortByInput: document.getElementById("sortByInput"),
        gaiaRadiusInput: document.getElementById("gaiaRadiusInput"),
        useCdppInput: document.getElementById("useCdppInput"),
        includeDvInput: document.getElementById("includeDvInput"),
        includeStatusInput: document.getElementById("includeStatusInput"),
        topNInput: document.getElementById("topNInput"),
        statusLine: document.getElementById("statusLine"),
        errorBox: document.getElementById("errorBox"),
        resultsBody: document.getElementById("resultsBody"),
        resultsTitle: document.getElementById("resultsTitle"),
        kpiSector: document.getElementById("kpiSector"),
        kpiCount: document.getElementById("kpiCount"),
        kpiRanking: document.getElementById("kpiRanking"),
        kpiApi: document.getElementById("kpiApi"),
        selectedTicLabel: document.getElementById("selectedTicLabel"),
        detailScore: document.getElementById("detailScore"),
        detailMesNorm: document.getElementById("detailMesNorm"),
        detailDepthScore: document.getElementById("detailDepthScore"),
        detailDurationScore: document.getElementById("detailDurationScore"),
        detailSnrNorm: document.getElementById("detailSnrNorm"),
        dvListWrap: document.getElementById("dvListWrap"),
        filterAllStatesInput: document.getElementById("filterAllStatesInput"),
        stateFilterInputs: document.querySelectorAll(".tceStateFilterInput"),
        filterAllVariabilityInput: document.getElementById("filterAllVariabilityInput"),
        variabilityFilterInputs: document.querySelectorAll(".tceVariabilityFilterInput"),
        filterSummary: document.getElementById("filterSummary"),
        projectNameInput: document.getElementById("projectNameInput"),
        projectSelect: document.getElementById("projectSelect"),
        saveProjectBtn: document.getElementById("saveProjectBtn"),
        loadProjectBtn: document.getElementById("loadProjectBtn"),
        deleteProjectBtn: document.getElementById("deleteProjectBtn"),
    };

    const USER_TCE_STATES = ["IN_ANALISI", "VALIDO", "NON_VALIDO"];
    const VARIABILITY_FILTER_VALUES = ["VARIABLE", "NON_VARIABLE"];
    const NOTE_MAX_LEN = 100;
    const PROJECT_INDEX_KEY = "agata_tess_tce_projects_index";
    const USER_TCE_STATE_LABEL = {
        IN_ANALISI: "in analisi",
        VALIDO: "valido",
        NON_VALIDO: "non valido",
    };

    const state = {
        items: [],
        selectedTic: null,
        selectedTceId: null,
        loading: false,
        batchLoading: false,
        detailViewer: null,
        expandedDocKindsByTic: {},
        selectedUserStates: new Set(USER_TCE_STATES),
        selectedVariabilityStates: new Set(VARIABILITY_FILTER_VALUES),
        initialQuery: {},
        initialSelectionApplied: false,
    };

    function bootstrapState() {
        const node = document.getElementById("tess-tce-bootstrap");
        if (!node) return {};
        try {
            return JSON.parse(node.textContent || "{}") || {};
        } catch (_) {
            return {};
        }
    }

    const bootstrap = bootstrapState();

    function apiBase() {
        return String(bootstrap.api_base || "").trim().replace(/\/+$/, "");
    }

    function normalizedInitialQuery() {
        const raw = bootstrap.initial_query || {};
        const out = {};
        ["tic_id", "toi", "target", "gaia_id", "ra", "dec"].forEach((key) => {
            const value = String(raw[key] || "").trim();
            if (value) out[key] = value;
        });
        return out;
    }

    function integrationSummary(query) {
        const parts = [];
        ["tic_id", "toi", "target", "gaia_id", "ra", "dec"].forEach((key) => {
            if (query[key]) parts.push(`${key}=${query[key]}`);
        });
        return parts.join(" | ");
    }

    function candidateTicIdsFromInitialQuery() {
        const query = state.initialQuery || {};
        const candidates = [];
        [query.tic_id, query.target].forEach((value) => {
            const normalized = String(value || "").trim();
            if (normalized && /^\d+$/.test(normalized) && !candidates.includes(normalized)) {
                candidates.push(normalized);
            }
        });
        return candidates;
    }

    function applyInitialSelectionIfPossible() {
        if (state.initialSelectionApplied || !state.items.length) return false;
        const candidates = candidateTicIdsFromInitialQuery();
        for (const ticId of candidates) {
            const row = state.items.find((item) => String(item.tic_id) === ticId);
            if (!row) continue;
            selectRowByTic(row.tic_id, row.tce_id || null);
            state.initialSelectionApplied = true;
            setStatus(`Contesto iniziale applicato: TIC ${ticId} selezionato dai parametri query.`);
            return true;
        }
        return false;
    }

    function normalizeSectorKey(sector) {
        const raw = String(sector ?? "").trim();
        return raw || "unknown";
    }

    function storageKeyForTce(sector, tceId) {
        return `agata_tess_tce_user_state:${normalizeSectorKey(sector)}:${String(tceId || "").trim()}`;
    }

    function storageKeyForTceNote(sector, tceId) {
        return `agata_tess_tce_note:${normalizeSectorKey(sector)}:${String(tceId || "").trim()}`;
    }

    function storageKeyForProject(name) {
        return `agata_tess_tce_project:${String(name || "").trim().toLowerCase()}`;
    }

    function loadUserTceState(sector, tceId) {
        try {
            const raw = localStorage.getItem(storageKeyForTce(sector, tceId));
            if (raw && USER_TCE_STATES.includes(raw)) return raw;
        } catch (_) {
            // ignore storage errors
        }
        return "IN_ANALISI";
    }

    function saveUserTceState(sector, tceId, value) {
        try {
            localStorage.setItem(storageKeyForTce(sector, tceId), value);
        } catch (_) {
            // ignore storage errors
        }
    }

    function loadTceNote(sector, tceId) {
        try {
            const raw = localStorage.getItem(storageKeyForTceNote(sector, tceId));
            if (!raw) return "";
            return String(raw).slice(0, NOTE_MAX_LEN);
        } catch (_) {
            return "";
        }
    }

    function saveTceNote(sector, tceId, noteValue) {
        const normalized = String(noteValue || "").slice(0, NOTE_MAX_LEN);
        try {
            localStorage.setItem(storageKeyForTceNote(sector, tceId), normalized);
        } catch (_) {
            // ignore storage errors
        }
        return normalized;
    }

    function setStatus(text) {
        if (ui.statusLine) ui.statusLine.textContent = text || "";
    }

    function setError(message) {
        if (!ui.errorBox) return;
        if (!message) {
            ui.errorBox.textContent = "";
            ui.errorBox.classList.add("hidden");
            return;
        }
        ui.errorBox.textContent = message;
        ui.errorBox.classList.remove("hidden");
    }

    function setKpis(payload) {
        if (ui.kpiSector) ui.kpiSector.textContent = payload && payload.sector != null ? String(payload.sector) : "-";
        if (ui.kpiCount) ui.kpiCount.textContent = payload && payload.count != null ? String(payload.count) : "-";
        if (ui.kpiRanking) ui.kpiRanking.textContent = payload && payload.ranking_version ? payload.ranking_version : "base_v1";
        if (ui.resultsTitle) {
            ui.resultsTitle.textContent = payload && payload.sector != null
                ? `Risultati TCE - Settore ${payload.sector}`
                : "Risultati TCE";
        }
    }

    function buildFastQuery() {
        const params = new URLSearchParams();
        params.set("sector", String(Number(ui.sectorInput.value || 0)));
        if (String(ui.limitInput.value || "").trim() !== "") {
            params.set("limit", String(Number(ui.limitInput.value)));
        }
        params.set("use_cdpp", String(!!ui.useCdppInput.checked));
        params.set("include_dv", String(!!ui.includeDvInput.checked));
        params.set("include_status", String(!!ui.includeStatusInput.checked));
        params.set("min_mes", String(Number(ui.minMesInput.value || 7.1)));
        params.set("sort_by", ui.sortByInput.value || "score");
        if (ui.maxDepthInput.value !== "") params.set("max_depth_pct", ui.maxDepthInput.value);
        if (ui.minDurationInput.value !== "") params.set("min_duration_hr", ui.minDurationInput.value);
        if (ui.maxDurationInput.value !== "") params.set("max_duration_hr", ui.maxDurationInput.value);
        return params;
    }

    function currentGaiaRadiusArcsec() {
        const value = Number(ui.gaiaRadiusInput && ui.gaiaRadiusInput.value);
        if (!Number.isFinite(value)) return 5;
        return Math.min(30, Math.max(0.5, value));
    }

    async function fetchJson(url, options) {
        const response = await fetch(url, options);
        const data = await response.json().catch(() => ({}));
        if (!response.ok) {
            throw new Error(data.error || data.detail || `HTTP ${response.status}`);
        }
        return data;
    }

    function statusBadgeClass(status) {
        switch (status) {
            case "PENDING":
            case "RETRY":
                return "badge badge-pending";
            case "NOT_REQUESTED":
                return "badge";
            case "CONFIRMED_PLANET":
            case "READY":
                return "badge badge-success";
            case "TOI":
                return "badge badge-info";
            case "FP":
                return "badge badge-error";
            case "UNAVAILABLE":
                return "badge badge-warning";
            case "TCE_ONLY":
                return "badge";
            default:
                return "badge badge-warning";
        }
    }

    function normalizedStatus(status, fallback) {
        return String(status || fallback || "").toUpperCase();
    }

    function displayStatusLabel(status) {
        switch (normalizedStatus(status)) {
            case "NOT_REQUESTED":
                return "on demand";
            case "RETRY":
                return "retry";
            case "UNAVAILABLE":
                return "no dv";
            default:
                return String(status || "-");
        }
    }

    function dvStatusMeta(row) {
        const status = normalizedStatus(row.dv_status, row.dv_available_unknown ? "NOT_REQUESTED" : "UNAVAILABLE");
        if (status === "READY") {
            return { status, label: "ready", title: "Documenti DV disponibili", className: "empty-note state-ready" };
        }
        if (status === "NOT_REQUESTED") {
            return { status, label: "on demand", title: "DV non ancora richiesti", className: "empty-note" };
        }
        if (status === "RETRY") {
            const detail = row.dv_error ? `Errore MAST temporaneo: ${row.dv_error}` : "Errore MAST temporaneo";
            return { status, label: "retry", title: detail, className: "empty-note state-retry" };
        }
        const reason = row.dv_error || "Nessun documento DV trovato in MAST";
        return { status, label: "no dv", title: reason, className: "empty-note state-unavailable" };
    }

    function dvErrorSummary(row) {
        const status = normalizedStatus(row && row.dv_status, row && row.dv_available_unknown ? "NOT_REQUESTED" : "UNAVAILABLE");
        if (status === "READY") return "Documenti DV disponibili";
        if (status === "NOT_REQUESTED") return "DV non ancora richiesti";
        const type = String((row && row.dv_error_type) || "").trim();
        const message = String((row && row.dv_error) || "").trim();
        if (!type && !message) {
            return status === "RETRY" ? "Errore temporaneo MAST" : "Nessun documento DV trovato";
        }
        const prefix = type || (status === "RETRY" ? "errore_temporaneo" : "nessun_dv");
        return message ? `${prefix}: ${message}` : prefix;
    }

    function primaryDoc(row) {
        if (!Array.isArray(row.dv_products) || row.dv_products.length === 0) return null;
        return row.dv_products.find((p) => p.kind === "DVR") || row.dv_products[0];
    }

    function isXmlProduct(product) {
        if (!product) return false;
        const filename = String(product.product_filename || "").toLowerCase();
        const dataUri = String(product.data_uri || "").toLowerCase();
        const url = String(product.mast_download_url || "").toLowerCase();
        return filename.endsWith(".xml") || dataUri.endsWith(".xml") || url.includes(".xml");
    }

    function isPdfProduct(product) {
        if (!product) return false;
        const filename = String(product.product_filename || "").toLowerCase();
        const dataUri = String(product.data_uri || "").toLowerCase();
        const url = String(product.mast_download_url || "").toLowerCase();
        return filename.endsWith(".pdf") || dataUri.endsWith(".pdf") || url.includes(".pdf");
    }

    function canPreviewProduct(product) {
        if (!product) return false;
        if (isXmlProduct(product)) return true;
        return String(product.kind || "").toUpperCase() === "DVS" && isPdfProduct(product);
    }

    function productKey(product) {
        if (!product) return "";
        return [product.kind || "", product.product_filename || "", product.mast_download_url || ""].join("|");
    }

    function findProductForRow(row, docKey) {
        if (!row || !Array.isArray(row.dv_products)) return null;
        return row.dv_products.find((p) => productKey(p) === docKey) || null;
    }

    function groupProductsByKind(products) {
        const out = { DVR: [], DVS: [], DVM: [], DVT: [], OTHER: [] };
        for (const p of (Array.isArray(products) ? products : [])) {
            const kind = String(p.kind || "OTHER").toUpperCase();
            if (out[kind]) out[kind].push(p);
            else out.OTHER.push(p);
        }
        return out;
    }

    function chunkArray(arr, size) {
        const out = [];
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
        return out;
    }

    function updateRowsForTic(ticId, patcher) {
        state.items = state.items.map((row) => row.tic_id === ticId ? patcher(row) : row);
    }

    function safeFixed(value, digits) {
        const n = Number(value);
        return Number.isFinite(n) ? n.toFixed(digits) : "-";
    }

    function dvCellHtml(row) {
        const doc = primaryDoc(row);
        if (doc) {
            const extra = isXmlProduct(doc)
                ? ` data-xml-viewer="1" data-xml-url="${escapeHtml(doc.mast_download_url)}" data-xml-filename="${escapeHtml(doc.product_filename || doc.kind)}"`
                : "";
            return `<a class="doc-link" href="${doc.mast_download_url}" target="_blank" rel="noreferrer" data-stop-row="1"${extra}>${doc.kind}</a>`;
        }
        const meta = dvStatusMeta(row);
        return `<span class="${meta.className}" title="${escapeHtml(meta.title)}">${escapeHtml(meta.label)}</span>`;
    }

    function userStateSelectHtml(row) {
        const current = USER_TCE_STATES.includes(row.user_state) ? row.user_state : "IN_ANALISI";
        const options = USER_TCE_STATES.map((s) =>
            `<option value="${s}"${s === current ? " selected" : ""}>${USER_TCE_STATE_LABEL[s]}</option>`
        ).join("");
        return `<select class="row-user-state-select" data-stop-row="1" data-tce-id="${escapeHtml(String(row.tce_id))}">${options}</select>`;
    }

    function variableKnownFromGaia(row) {
        return !!(row && row.gaia_lookup && row.gaia_lookup.gaia_variable_known === true);
    }

    function variableKnownFromVsx(row) {
        return !!(row && row.vsx_lookup && row.vsx_lookup.vsx_variable_known === true);
    }

    function gaiaNegativeVariability(row) {
        return !!(row && row.gaia_lookup && row.gaia_lookup.status === "OK" && row.gaia_lookup.gaia_variable_known === false);
    }

    function vsxNegativeVariability(row) {
        return !!(
            row
            && row.vsx_lookup
            && (
                row.vsx_lookup.status === "NOT_FOUND"
                || (row.vsx_lookup.status === "OK" && row.vsx_lookup.vsx_variable_known === false)
            )
        );
    }

    function variabilityClass(row) {
        if (variableKnownFromGaia(row) || variableKnownFromVsx(row)) return "VARIABLE";
        if (gaiaNegativeVariability(row) && vsxNegativeVariability(row)) return "NON_VARIABLE";
        return "UNVERIFIED";
    }

    function variabilityLabel(row) {
        switch (variabilityClass(row)) {
            case "VARIABLE":
                return "var.";
            case "NON_VARIABLE":
                return "non var.";
            default:
                return "non verif.";
        }
    }

    function hasDvLoaded(row) {
        if (!row) return false;
        if (Array.isArray(row.dv_products)) return true;
        return normalizedStatus(row.dv_status, row.dv_available_unknown ? "NOT_REQUESTED" : "UNAVAILABLE") !== "NOT_REQUESTED";
    }

    function isLookupSettledForRadius(lookup, requestedRadiusArcsec) {
        if (!lookup) return false;
        if (!["OK", "NOT_FOUND", "ERROR"].includes(String(lookup.status || ""))) return false;
        return Number(lookup.search_radius_arcsec) === requestedRadiusArcsec;
    }

    function needsVariabilityLookup(row) {
        const requestedRadiusArcsec = currentGaiaRadiusArcsec();
        return !(
            isLookupSettledForRadius(row && row.gaia_lookup, requestedRadiusArcsec)
            && isLookupSettledForRadius(row && row.vsx_lookup, requestedRadiusArcsec)
        );
    }

    function topUniqueTicsByPredicate(limit, predicate) {
        const out = [];
        const seen = new Set();
        for (const row of state.items) {
            const ticId = String(row && row.tic_id || "").trim();
            if (!ticId || seen.has(ticId)) continue;
            if (!predicate(row)) continue;
            seen.add(ticId);
            out.push(ticId);
            if (out.length >= limit) break;
        }
        return out;
    }

    function filteredItems() {
        if (state.selectedUserStates.size === 0) return [];
        return state.items.filter((row) => {
            if (!state.selectedUserStates.has(String(row.user_state || "IN_ANALISI"))) return false;
            const vClass = variabilityClass(row);
            if (vClass === "VARIABLE") return state.selectedVariabilityStates.has("VARIABLE");
            if (vClass === "NON_VARIABLE") return state.selectedVariabilityStates.has("NON_VARIABLE");
            return true;
        });
    }

    function updateFilterSummary() {
        if (!ui.filterSummary) return;
        const total = state.items.length;
        const visible = filteredItems().length;
        ui.filterSummary.textContent = `${visible}/${total} visibili`;
    }

    function renderRows() {
        if (!ui.resultsBody) return;
        if (!Array.isArray(state.items) || state.items.length === 0) {
            ui.resultsBody.innerHTML = '<tr><td colspan="12" class="empty-cell">Nessun dato. Premi Run.</td></tr>';
            updateFilterSummary();
            return;
        }
        const rows = filteredItems();
        if (!rows.length) {
            ui.resultsBody.innerHTML = '<tr><td colspan="12" class="empty-cell">Nessun TCE con i filtri correnti.</td></tr>';
            updateFilterSummary();
            return;
        }
        ui.resultsBody.innerHTML = rows.map((row) => {
            const docsHtml = dvCellHtml(row);
            const variabilityHtml = escapeHtml(variabilityLabel(row));
            const isSelected = state.selectedTceId
                ? state.selectedTceId === row.tce_id
                : state.selectedTic === row.tic_id;
            const trClass = isSelected ? ' class="selected-row"' : "";
            return `<tr data-tic-id="${row.tic_id}" data-tce-id="${escapeHtml(String(row.tce_id))}"${trClass}>
                <td data-col="rank">${row.rank ?? "-"}</td>
                <td data-col="score">${Number(row.score).toFixed(4)}</td>
                <td class="mono" data-col="tic_id">${escapeHtml(String(row.tic_id))}</td>
                <td data-col="tce_id" title="${escapeHtml(String(row.tce_id))}">${escapeHtml(String(row.tce_id))}</td>
                <td data-col="period">${row.period_days == null ? "-" : Number(row.period_days).toFixed(4)}</td>
                <td data-col="duration_hr">${Number(row.duration_hr).toFixed(2)}</td>
                <td data-col="depth_pct">${Number(row.depth_pct).toFixed(3)}</td>
                <td data-col="mes">${Number(row.mes).toFixed(2)}</td>
                <td data-col="status"><span class="${statusBadgeClass(normalizedStatus(row.status, row.status_available_unknown ? "NOT_REQUESTED" : "UNKNOWN"))}" title="${escapeHtml(row.status_reason || "")}">${escapeHtml(displayStatusLabel(normalizedStatus(row.status, row.status_available_unknown ? "NOT_REQUESTED" : "UNKNOWN")))}</span></td>
                <td data-col="user_state">${userStateSelectHtml(row)}</td>
                <td data-col="docs">${docsHtml}</td>
                <td data-col="var">${variabilityHtml}</td>
            </tr>`;
        }).join("");
        updateFilterSummary();
    }

    function escapeHtml(text) {
        return String(text)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function currentDetailRow() {
        if (state.selectedTceId != null) {
            const byTce = state.items.find((x) => x.tce_id === state.selectedTceId);
            if (byTce) return byTce;
        }
        if (state.selectedTic == null) return null;
        return state.items.find((x) => x.tic_id === state.selectedTic) || null;
    }

    function getXmlSummaryValue(row, label) {
        const items = Array.isArray(row && row.xml_summary_items) ? row.xml_summary_items : [];
        const hit = items.find((x) => String(x.label || "").toLowerCase() === String(label).toLowerCase());
        return hit ? String(hit.value || "") : "";
    }

    function computeSepArcsecFromRow(row) {
        if (!row || !row.gaia_lookup) return null;
        const gaiaRa = Number(row.gaia_lookup.gaia_source_ra_deg);
        const gaiaDec = Number(row.gaia_lookup.gaia_source_dec_deg);
        const xmlRa = Number(String(getXmlSummaryValue(row, "RA [deg]") || "").replace(",", "."));
        const xmlDec = Number(String(getXmlSummaryValue(row, "Dec [deg]") || "").replace(",", "."));
        if (![gaiaRa, gaiaDec, xmlRa, xmlDec].every(Number.isFinite)) return null;
        const toRad = (deg) => (deg * Math.PI) / 180;
        const dra = (gaiaRa - xmlRa) * Math.cos(toRad((gaiaDec + xmlDec) / 2));
        const ddec = gaiaDec - xmlDec;
        return Math.sqrt(dra * dra + ddec * ddec) * 3600;
    }

    function detailStatusHtml(row) {
        const title = escapeHtml(row.status_reason || "");
        const status = normalizedStatus(row.status, row.status_available_unknown ? "NOT_REQUESTED" : "UNKNOWN");
        return `<span class="${statusBadgeClass(status)}" title="${title}">${escapeHtml(displayStatusLabel(status))}</span>`;
    }

    function renderXmlSummaryCard(summaryItems) {
        if (!Array.isArray(summaryItems) || !summaryItems.length) {
            return '<div class="empty-cell">Nessuna informazione XML ancora caricata. Seleziona una istanza XML e premi Apri.</div>';
        }
        const colCount = summaryItems.length >= 10 ? 3 : 2;
        const rows = chunkArray(summaryItems, colCount);
        const headCells = Array.from({ length: colCount }, () => '<th>Campo</th><th>Valore</th>').join("");
        const body = rows.map((rowItems) => {
            const cells = rowItems.map((r) => `<td class="xml-k">${escapeHtml(r.label)}</td><td class="xml-v">${escapeHtml(r.value)}</td>`).join("");
            const filler = Array.from({ length: colCount - rowItems.length }, () => '<td></td><td></td>').join("");
            return `<tr>${cells}${filler}</tr>`;
        }).join("");
        return `<table class="kv compact-kv xml-grid-table"><thead><tr>${headCells}</tr></thead><tbody>${body}</tbody></table>`;
    }

    function renderDocsGrouped(row) {
        const products = Array.isArray(row.dv_products) ? row.dv_products : [];
        const dvMeta = dvStatusMeta(row);
        if (row.dv_products == null && normalizedStatus(row.dv_status, "NOT_REQUESTED") === "NOT_REQUESTED") {
            return '<div class="empty-cell">DV non ancora richiesti. Usa Fetch DV riga o Fetch DV Top N.</div>';
        }
        if (row.dv_error && (!products.length)) {
            return `<div class="empty-cell">DV ${escapeHtml(dvMeta.label)}: ${escapeHtml(dvErrorSummary(row))}</div>`;
        }
        if (!products.length) {
            return `<div class="empty-cell">${escapeHtml(dvMeta.title)}</div>`;
        }

        const groups = groupProductsByKind(products);
        const orderedKinds = ["DVR", "DVS", "DVM", "DVT", "OTHER"];
        const selectedKey = state.detailViewer && state.detailViewer.ticId === row.tic_id ? state.detailViewer.docKey : "";
        const expandedMap = state.expandedDocKindsByTic[row.tic_id] || {};
        return orderedKinds
            .filter((kind) => groups[kind].length)
            .map((kind) => {
                const hasExplicitState = Object.prototype.hasOwnProperty.call(expandedMap, kind);
                const isExpanded = hasExplicitState
                    ? !!expandedMap[kind]
                    : groups[kind].some((p) => productKey(p) === selectedKey);
                const instances = groups[kind].map((p, idx) => {
                    const key = productKey(p);
                    const selectedClass = selectedKey === key ? " is-selected" : "";
                    const previewable = canPreviewProduct(p);
                    const fileName = escapeHtml(p.product_filename || `${kind} #${idx + 1}`);
                    const downloadHref = `${apiBase()}/download?url=${encodeURIComponent(p.mast_download_url)}`;
                    const previewBtn = previewable
                        ? `<button type="button" class="doc-action-btn" data-stop-row="1" data-dv-action="preview" data-doc-key="${escapeHtml(key)}">Apri</button>`
                        : "";
                    const downloadBtn = `<a class="doc-action-link" href="${downloadHref}" target="_blank" rel="noreferrer" data-stop-row="1">Download</a>`;
                    return `<li class="dv-doc-item${selectedClass}">
                        <div class="dv-doc-main">
                            <button type="button" class="doc-select-btn" data-stop-row="1" data-dv-action="select" data-doc-key="${escapeHtml(key)}">${fileName}</button>
                            <span class="dv-doc-meta">${isXmlProduct(p) ? "XML" : (isPdfProduct(p) ? "PDF" : "file")}</span>
                        </div>
                        <div class="dv-doc-actions">${previewBtn}${downloadBtn}</div>
                    </li>`;
                }).join("");
                return `<section class="dv-group ${isExpanded ? "is-expanded" : "is-collapsed"}">
                    <button type="button" class="dv-group-title dv-group-toggle" data-stop-row="1" data-dv-action="toggle-kind" data-doc-kind="${kind}" aria-expanded="${isExpanded ? "true" : "false"}">
                        <span class="badge">${kind}</span>
                        <span class="dv-group-count">${groups[kind].length} istanze</span>
                        <span class="dv-group-chevron" aria-hidden="true">${isExpanded ? "▾" : "▸"}</span>
                    </button>
                    <ul class="dv-doc-list" ${isExpanded ? "" : 'hidden'}>${instances}</ul>
                </section>`;
            }).join("");
    }

    function renderViewer(row) {
        const viewer = state.detailViewer;
        if (!viewer || viewer.ticId !== row.tic_id) {
            return '<div class="empty-cell">Seleziona una istanza documento per vedere anteprima o azioni disponibili.</div>';
        }
        if (viewer.loading) {
            return '<div class="empty-cell">Caricamento documento...</div>';
        }
        if (viewer.error) {
            return `<div class="empty-cell">Errore documento: ${escapeHtml(viewer.error)}</div>`;
        }
        if (viewer.mode === "xml") {
            const rawXml = viewer.rawXml || "";
            return `<div class="doc-viewer-head">
                <div><strong>${escapeHtml(viewer.filename || "XML DV")}</strong></div>
                <div class="doc-viewer-actions">
                    <a class="doc-action-link" href="${apiBase()}/download?url=${encodeURIComponent(viewer.url || "")}" target="_blank" rel="noreferrer">Download XML</a>
                </div>
            </div>
            <div class="xml-readable">${viewer.summaryHtml || '<div class="empty-cell">Nessun riepilogo disponibile.</div>'}</div>
            <details class="xml-raw-details"><summary>XML raw (formattato)</summary><pre>${escapeHtml(rawXml)}</pre></details>`;
        }
        if (viewer.mode === "pdf") {
            const pdfSrc = `${apiBase()}/download?url=${encodeURIComponent(viewer.url || "")}&disposition=inline&filename=${encodeURIComponent(viewer.filename || "document.pdf")}#zoom=page-fit`;
            return `<div class="doc-viewer-head">
                <div><strong>${escapeHtml(viewer.filename || "DVS")}</strong></div>
                <div class="doc-viewer-actions">
                    <a class="doc-action-link" href="${apiBase()}/download?url=${encodeURIComponent(viewer.url || "")}" target="_blank" rel="noreferrer">Download</a>
                </div>
            </div>
            <iframe class="doc-iframe" src="${pdfSrc}" title="${escapeHtml(viewer.filename || "DVS PDF")}"></iframe>`;
        }
        return `<div class="empty-cell">Preview non disponibile per questo documento. Usa Download.</div>`;
    }

    function setDetail(row) {
        if (!row) {
            ui.selectedTicLabel.textContent = "Nessuna selezione";
            ui.detailScore.textContent = "-";
            ui.detailMesNorm.textContent = "-";
            ui.detailDepthScore.textContent = "-";
            ui.detailDurationScore.textContent = "-";
            ui.detailSnrNorm.textContent = "-";
            state.detailViewer = null;
            ui.dvListWrap.innerHTML = '<div class="empty-cell">Nessun TIC selezionato.</div>';
            return;
        }
        ui.selectedTicLabel.textContent = `TIC ${row.tic_id} • TCE ${row.tce_id}`;
        ui.detailScore.textContent = safeFixed(row.score, 6);
        ui.detailMesNorm.textContent = safeFixed(row.ranking_components && row.ranking_components.mes_norm, 4);
        ui.detailDepthScore.textContent = safeFixed(row.ranking_components && row.ranking_components.depth_score, 4);
        ui.detailDurationScore.textContent = safeFixed(row.ranking_components && row.ranking_components.duration_score, 4);
        ui.detailSnrNorm.textContent = safeFixed(row.ranking_components && row.ranking_components.snr_norm, 4);

        const xmlSummaryItems = Array.isArray(row.xml_summary_items) ? row.xml_summary_items : [];
        const gaia = row.gaia_lookup || null;
        const vsx = row.vsx_lookup || null;
        const gaiaStatusBadge = gaia
            ? `<span class="${statusBadgeClass(
                gaia.status === "OK" ? "TOI" : gaia.status === "NOT_FOUND" ? "TCE_ONLY" : "UNKNOWN"
            )}">${escapeHtml(gaia.status || "UNKNOWN")}</span>`
            : '<span class="badge badge-pending">PENDING</span>';
        const vsxStatusBadge = vsx
            ? `<span class="${statusBadgeClass(
                vsx.status === "OK" ? "TOI" : vsx.status === "NOT_FOUND" ? "TCE_ONLY" : "UNKNOWN"
            )}">${escapeHtml(vsx.status || "UNKNOWN")}</span>`
            : '<span class="badge badge-pending">PENDING</span>';
        const dvMeta = dvStatusMeta(row);
        const gaiaErrorText = gaia && gaia.status === "ERROR" ? String(gaia.reason || "Errore Gaia lookup") : "-";
        const vsxErrorText = vsx && vsx.status === "ERROR" ? String(vsx.reason || "Errore VSX lookup") : "-";
        const gaiaVarText = !gaia
            ? "Da ricavare da RA/Dec XML"
            : gaia.gaia_variable_known === true
                ? `Si${gaia.gaia_variability_catalog ? ` (${gaia.gaia_variability_catalog})` : ""}`
            : gaia.gaia_variable_known === false
                    ? "No"
                    : "-";
        const vsxVarText = !vsx
            ? "Da ricavare da coord. TIC"
            : vsx.vsx_variable_known === true
                ? `Si${vsx.vsx_catalog ? ` (${vsx.vsx_catalog})` : ""}`
                : vsx.vsx_variable_known === false
                    ? "No"
                    : "-";
        const sepArcsecComputed = computeSepArcsecFromRow(row);
        const sepDisplay = gaia && gaia.sep_arcsec != null
            ? Number(gaia.sep_arcsec).toFixed(3)
            : (sepArcsecComputed != null ? Number(sepArcsecComputed).toFixed(3) : "-");
        const vsxSepDisplay = vsx && vsx.sep_arcsec != null ? Number(vsx.sep_arcsec).toFixed(3) : "-";
        const gaiaIdDisplay = gaia && gaia.source_id
            ? `${String(gaia.source_id)}${gaia.gaia_release ? ` (${gaia.gaia_release})` : ""}`
            : "-";
        const ticCoordsDisplay = gaia && gaia.tic_ra_deg != null && gaia.tic_dec_deg != null
            ? `${safeFixed(gaia.tic_ra_deg, 6)} / ${safeFixed(gaia.tic_dec_deg, 6)}`
            : "-";
        const gaiaCoordsDisplay = gaia && gaia.gaia_source_ra_deg != null && gaia.gaia_source_dec_deg != null
            ? `${safeFixed(gaia.gaia_source_ra_deg, 6)} / ${safeFixed(gaia.gaia_source_dec_deg, 6)}`
            : "-";
        const gaiaVarDetail = gaia && gaia.gaia_variable_known === true
            ? [
                gaia.gaia_variability_type ? `tipo=${gaia.gaia_variability_type}` : null,
                gaia.gaia_variability_period_days != null ? `periodo=${safeFixed(gaia.gaia_variability_period_days, 6)} d` : null,
                sepDisplay !== "-" ? `sep=${sepDisplay}"` : null,
            ].filter(Boolean).join(" | ")
            : "-";
        const vsxVarDetail = vsx && vsx.vsx_variable_known === true
            ? [
                vsx.vsx_type ? `tipo=${vsx.vsx_type}` : null,
                vsx.vsx_period_days != null ? `periodo=${safeFixed(vsx.vsx_period_days, 6)} d` : null,
                vsxSepDisplay !== "-" ? `sep=${vsxSepDisplay}"` : null,
            ].filter(Boolean).join(" | ")
            : "-";
        const schedaTicHtml = `<section class="detail-card">
                <h3>Scheda TIC</h3>
                <div class="tic-info-grid">
                    <div><span class="k">TIC</span><span class="v mono">${escapeHtml(String(row.tic_id))}</span></div>
                    <div><span class="k">Status TAP</span><span class="v">${detailStatusHtml(row)}</span></div>
                    <div><span class="k">TCE selezionato</span><span class="v">${escapeHtml(String(row.tce_id || "-"))}</span></div>
                    <div><span class="k">DV</span><span class="v"><span class="${statusBadgeClass(dvMeta.status)}">${escapeHtml(displayStatusLabel(dvMeta.status))}</span></span></div>
                    <div><span class="k">DV dettaglio</span><span class="v">${escapeHtml(dvErrorSummary(row))}</span></div>
                    <div><span class="k">Coord. TIC cat.</span><span class="v mono">${escapeHtml(ticCoordsDisplay)}</span></div>
                    <div><span class="k">Coord. Gaia src</span><span class="v mono">${escapeHtml(gaiaCoordsDisplay)}</span></div>
                    <div><span class="k">RA / Dec (XML)</span><span class="v mono">${escapeHtml(getXmlSummaryValue(row, "RA [deg]") || "-")} / ${escapeHtml(getXmlSummaryValue(row, "Dec [deg]") || "-")}</span></div>
                    <div><span class="k">Raggio (XML)</span><span class="v">${escapeHtml(getXmlSummaryValue(row, "Raggio stella [Rsun]") || getXmlSummaryValue(row, "Raggio pianeta [Rearth]") || "-")}</span></div>
                    <div><span class="k">Gaia lookup</span><span class="v">${gaiaStatusBadge}</span></div>
                    <div><span class="k">Gaia source_id</span><span class="v mono">${escapeHtml(gaiaIdDisplay)}</span></div>
                    <div><span class="k">Variabile nota (Gaia)</span><span class="v">${escapeHtml(gaiaVarText)}</span></div>
                    <div><span class="k">Errore Gaia</span><span class="v">${escapeHtml(gaiaErrorText)}</span></div>
                    <div><span class="k">Dettaglio variab.</span><span class="v">${escapeHtml(gaiaVarDetail)}</span></div>
                    <div><span class="k">Sep. Gaia [arcsec]</span><span class="v">${escapeHtml(sepDisplay)}</span></div>
                    <div><span class="k">VSX lookup</span><span class="v">${vsxStatusBadge}</span></div>
                    <div><span class="k">Variabile nota (VSX)</span><span class="v">${escapeHtml(vsxVarText)}</span></div>
                    <div><span class="k">Errore VSX</span><span class="v">${escapeHtml(vsxErrorText)}</span></div>
                    <div><span class="k">Dettaglio VSX</span><span class="v">${escapeHtml(vsxVarDetail)}</span></div>
                    <div><span class="k">Raggio ricerca</span><span class="v">${escapeHtml(gaia && gaia.search_radius_arcsec != null ? `${safeFixed(gaia.search_radius_arcsec, 1)}"` : `${safeFixed(currentGaiaRadiusArcsec(), 1)}"` )}</span></div>
                </div>
            </section>`;

        const noteValue = String(row.user_note || "");
        const noteHtml = `<section class="detail-card">
            <h3>Nota TCE</h3>
            <div class="tce-note-wrap">
                <textarea class="tce-note-input" data-note-tce-id="${escapeHtml(String(row.tce_id))}" maxlength="${NOTE_MAX_LEN}" placeholder="Inserisci nota sintetica per questo TCE...">${escapeHtml(noteValue)}</textarea>
                <div class="tce-note-actions">
                    <span class="meta"><span class="tce-note-count">${noteValue.length}</span>/${NOTE_MAX_LEN}</span>
                    <button type="button" class="doc-action-btn" data-stop-row="1" data-dv-action="save-note" data-note-tce-id="${escapeHtml(String(row.tce_id))}">Salva nota</button>
                </div>
            </div>
        </section>`;

        const xmlInfoHtml = `<section class="detail-card">
                <h3>Info rilevanti da XML (istanza selezionata)</h3>
                ${renderXmlSummaryCard(xmlSummaryItems)}
            </section>`;

        const docsHtml = `<section class="detail-card">
            <h3>Documenti disponibili (per tipo e istanze)</h3>
            ${renderDocsGrouped(row)}
        </section>`;

        const viewerHtml = `<section class="detail-card">
            <h3>Viewer documento selezionato</h3>
            ${renderViewer(row)}
        </section>`;

        ui.dvListWrap.innerHTML = `<div class="detail-layout">${schedaTicHtml}${xmlInfoHtml}${noteHtml}${viewerHtml}${docsHtml}</div>`;
    }

    function prettyPrintXml(xml) {
        const normalized = String(xml || "").replace(/>\s+</g, "><").trim();
        if (!normalized) return "";
        const tokens = normalized.replace(/(>)(<)(\/*)/g, "$1\n$2$3").split("\n");
        let pad = 0;
        const lines = [];
        for (const token of tokens) {
            if (!token) continue;
            let indentChangeBefore = 0;
            let indentChangeAfter = 0;
            if (/^<\//.test(token)) {
                indentChangeBefore = -1;
            } else if (/^<[^!?][^>]*[^/]?>$/.test(token) && !/^<[^>]+>.*<\/[^>]+>$/.test(token)) {
                indentChangeAfter = 1;
            }
            pad = Math.max(0, pad + indentChangeBefore);
            lines.push("  ".repeat(pad) + token);
            pad = Math.max(0, pad + indentChangeAfter);
        }
        return lines.join("\n");
    }

    function firstNodeWithAttr(xmlDoc, tagNames, attrName) {
        for (const tag of tagNames) {
            const nodes = xmlDoc.getElementsByTagName(tag);
            if (!nodes || !nodes.length) continue;
            for (const node of nodes) {
                const val = node.getAttribute && node.getAttribute(attrName);
                if (val != null && String(val).trim() !== "") return String(val).trim();
            }
        }
        return "";
    }

    function firstNodeAttrByName(xmlDoc, tagNames, nameAttrValue, targetAttrName) {
        for (const tag of tagNames) {
            const nodes = xmlDoc.getElementsByTagName(tag);
            if (!nodes || !nodes.length) continue;
            for (const node of nodes) {
                const nameVal = node.getAttribute && node.getAttribute("name");
                if (String(nameVal || "").trim() !== String(nameAttrValue || "").trim()) continue;
                const targetVal = node.getAttribute && node.getAttribute(targetAttrName);
                if (targetVal != null && String(targetVal).trim() !== "") return String(targetVal).trim();
            }
        }
        return "";
    }

    function buildReadableXmlHtml(xmlText) {
        const parser = new DOMParser();
        const xmlDoc = parser.parseFromString(String(xmlText || ""), "application/xml");
        if (xmlDoc.getElementsByTagName("parsererror").length) {
            return {
                summaryHtml: `<div class="warn">Parsing XML non riuscito: mostro il contenuto raw.</div>`,
                rawXml: prettyPrintXml(xmlText || ""),
            };
        }
        const root = xmlDoc.documentElement;
        const kv = [];
        const pushIf = (label, value) => {
            if (value != null && String(value).trim() !== "") kv.push({ label, value: String(value).trim() });
        };
        pushIf("TIC ID", root?.getAttribute("ticId"));
        pushIf("Pipeline Task ID", root?.getAttribute("pipelineTaskId"));
        pushIf("Cadence iniziale", root?.getAttribute("startCadence"));
        pushIf("Cadence finale", root?.getAttribute("endCadence"));
        pushIf("Candidate count", root?.getAttribute("planetCandidateCount"));
        pushIf("Sectors observed", root?.getAttribute("sectorsObserved"));
        pushIf("RA [deg]", root?.getAttribute("raDegrees"));
        pushIf("Dec [deg]", root?.getAttribute("decDegrees"));
        pushIf("Raggio stella [Rsun]", firstNodeWithAttr(xmlDoc, ["dv:radius", "radius", "dv:stellarRadius", "stellarRadius"], "value"));
        pushIf(
            "Raggio pianeta [Rearth]",
            firstNodeAttrByName(xmlDoc, ["dv:modelParameter", "modelParameter"], "planetRadiusEarthRadii", "value")
        );
        pushIf("Teff [K]", firstNodeWithAttr(xmlDoc, ["dv:effectiveTemp", "effectiveTemp"], "value"));
        pushIf("log(g)", firstNodeWithAttr(xmlDoc, ["dv:log10SurfaceGravity", "log10SurfaceGravity"], "value"));
        pushIf("Metallicita'", firstNodeWithAttr(xmlDoc, ["dv:log10Metallicity", "log10Metallicity"], "value"));
        pushIf("Planet number", firstNodeWithAttr(xmlDoc, ["dv:planetResults", "planetResults"], "planetNumber"));
        pushIf("Transit model", firstNodeWithAttr(xmlDoc, ["dv:allTransitsFit", "allTransitsFit"], "transitModelName"));

        const summaryTable = kv.length
            ? `<table class="kv">${kv.map((r) => `<tr><th>${escapeHtml(r.label)}</th><td>${escapeHtml(r.value)}</td></tr>`).join("")}</table>`
            : `<div class="warn">Nessun campo chiave riconosciuto automaticamente.</div>`;
        return {
            summaryHtml: `<section class="card"><h3>Riepilogo leggibile</h3>${summaryTable}</section>`,
            rawXml: prettyPrintXml(xmlText || ""),
            summaryItems: kv,
        };
    }

    async function openXmlViewer(xmlUrl, filename) {
        const popup = window.open("", "_blank", "width=980,height=780,resizable=yes,scrollbars=yes");
        if (!popup) {
            setStatus("Popup bloccato dal browser. Consenti popup per visualizzare XML.");
            return;
        }
        popup.document.write("<!doctype html><title>Caricamento XML...</title><body style='font-family:Segoe UI,Tahoma,sans-serif;padding:16px'>Caricamento XML...</body>");
        try {
            const payload = await fetchJson(`${apiBase()}/xml-view?url=${encodeURIComponent(xmlUrl)}`);
            const parsed = buildReadableXmlHtml(payload.xml_text || "");
            const escaped = escapeHtml(parsed.rawXml || payload.xml_text || "");
            popup.document.open();
            popup.document.write(
                `<!doctype html>
                <html lang="it"><head><meta charset="utf-8"><title>${escapeHtml(filename || "XML DV")}</title>
                <style>
                body{margin:0;font-family:Segoe UI,Tahoma,sans-serif;background:#f3f6fa;color:#223244}
                header{padding:12px 16px;background:#22364d;color:#fff}
                .meta{padding:10px 16px;color:#55697d;font-size:13px;background:#eef4fb;border-bottom:1px solid #d8e3f0}
                .wrap{padding:16px;display:grid;gap:12px}
                .card{background:#fff;border:1px solid #dbe5f0;border-radius:10px;padding:12px}
                .card h3{margin:0 0 8px;font-size:15px}
                .warn{padding:10px;background:#fff7e6;border-left:4px solid #d39b2c;border-radius:8px;color:#7b5a15}
                table.kv{width:100%;border-collapse:collapse;font-size:13px}
                table.kv th,table.kv td{border-bottom:1px solid #edf2f7;padding:6px 8px;text-align:left;vertical-align:top}
                table.kv th{width:260px;color:#4e6175;font-weight:600}
                details{background:#fff;border:1px solid #dbe5f0;border-radius:10px;padding:10px}
                details summary{cursor:pointer;font-weight:600;color:#294766}
                pre{margin:0;padding:12px;white-space:pre-wrap;word-break:break-word;font:12px/1.45 Consolas,'Courier New',monospace;background:#f8fbff;border:1px solid #e3edf7;border-radius:8px}
                </style></head>
                <body>
                <header><strong>${escapeHtml(filename || "XML DV")}</strong></header>
                <div class="meta">${escapeHtml(payload.url || "")}</div>
                <div class="wrap">
                  ${parsed.summaryHtml}
                  <details>
                    <summary>XML raw (formattato)</summary>
                    <pre>${escaped}</pre>
                  </details>
                </div>
                </body></html>`
            );
            popup.document.close();
        } catch (err) {
            popup.document.open();
            popup.document.write(
                `<!doctype html><html><body style="font-family:Segoe UI,Tahoma,sans-serif;padding:16px;color:#8e2626">
                <h2>Errore apertura XML</h2>
                <pre style="white-space:pre-wrap">${escapeHtml(err instanceof Error ? err.message : String(err))}</pre>
                </body></html>`
            );
            popup.document.close();
        }
    }

    function selectRowByTic(ticId, tceId) {
        state.selectedTic = ticId;
        state.selectedTceId = tceId || null;
        renderRows();
        const row = tceId
            ? (state.items.find((x) => x.tce_id === tceId) || state.items.find((x) => x.tic_id === ticId) || null)
            : (state.items.find((x) => x.tic_id === ticId) || null);
        setDetail(row);
    }

    async function previewDocumentForTic(ticId, docKey) {
        const row = state.items.find((x) => x.tic_id === ticId);
        if (!row) return;
        const product = findProductForRow(row, docKey);
        if (!product) return;
        const filename = product.product_filename || product.kind || "document";
        const url = product.mast_download_url || "";

        state.detailViewer = {
            ticId,
            docKey,
            filename,
            url,
            mode: "none",
            loading: true,
            error: null,
        };
        setDetail(row);

        try {
            if (isXmlProduct(product)) {
                const payload = await fetchJson(`${apiBase()}/xml-view?url=${encodeURIComponent(url)}`);
                const parsed = buildReadableXmlHtml(payload.xml_text || "");
                const summaryItems = parsed.summaryItems || [];
                updateRowsForTic(ticId, (item) => ({ ...item, xml_summary_items: summaryItems }));
                await ensureGaiaLookupForTic(ticId);
                state.detailViewer = {
                    ticId,
                    docKey,
                    filename,
                    url,
                    mode: "xml",
                    loading: false,
                    error: null,
                    summaryHtml: parsed.summaryHtml || "",
                    rawXml: parsed.rawXml || "",
                };
                const currentMap = state.expandedDocKindsByTic[ticId] || {};
                state.expandedDocKindsByTic[ticId] = { ...currentMap, [String(product.kind || "OTHER").toUpperCase()]: true };
            } else if (String(product.kind || "").toUpperCase() === "DVS" && isPdfProduct(product)) {
                state.detailViewer = {
                    ticId,
                    docKey,
                    filename,
                    url,
                    mode: "pdf",
                    loading: false,
                    error: null,
                };
                const currentMap = state.expandedDocKindsByTic[ticId] || {};
                state.expandedDocKindsByTic[ticId] = { ...currentMap, DVS: true };
            } else {
                state.detailViewer = {
                    ticId,
                    docKey,
                    filename,
                    url,
                    mode: "none",
                    loading: false,
                    error: "Preview non supportata per questo tipo di documento.",
                };
            }
        } catch (err) {
            state.detailViewer = {
                ticId,
                docKey,
                filename,
                url,
                mode: "none",
                loading: false,
                error: err instanceof Error ? err.message : String(err),
            };
        }
        const current = currentDetailRow();
        if (current) setDetail(current);
    }

    async function ensureGaiaLookupForTic(ticId) {
        const current = state.items.find((x) => x.tic_id === ticId);
        if (!current) return;
        const requestedRadiusArcsec = currentGaiaRadiusArcsec();
        if (
            current.gaia_lookup
            && ["OK", "NOT_FOUND", "ERROR"].includes(String(current.gaia_lookup.status || ""))
            && Number(current.gaia_lookup.search_radius_arcsec) === requestedRadiusArcsec
        ) {
            return;
        }
        updateRowsForTic(ticId, (item) => ({
            ...item,
            gaia_lookup: {
                status: "PENDING",
                reason: `Lookup Gaia/VizieR in corso (r=${requestedRadiusArcsec.toFixed(1)}")...`,
                tic_id: ticId,
                search_radius_arcsec: requestedRadiusArcsec,
            },
        }));
        renderRows();
        const rowBefore = currentDetailRow();
        if (rowBefore && rowBefore.tic_id === ticId) setDetail(rowBefore);
        try {
            const payload = await fetchJson(
                `${apiBase()}/gaia-lookup?tic_id=${encodeURIComponent(String(ticId))}&radius_arcsec=${encodeURIComponent(String(requestedRadiusArcsec))}`
            );
            updateRowsForTic(ticId, (item) => ({ ...item, gaia_lookup: payload || { status: "UNKNOWN" } }));
        } catch (err) {
            updateRowsForTic(ticId, (item) => ({
                ...item,
                gaia_lookup: {
                    status: "ERROR",
                    reason: err instanceof Error ? err.message : String(err),
                    tic_id: ticId,
                    search_radius_arcsec: requestedRadiusArcsec,
                },
            }));
        }
        renderRows();
        const rowAfter = currentDetailRow();
        if (rowAfter && rowAfter.tic_id === ticId) setDetail(rowAfter);
    }

    async function ensureVsxLookupForTic(ticId) {
        const current = state.items.find((x) => x.tic_id === ticId);
        if (!current) return;
        const requestedRadiusArcsec = currentGaiaRadiusArcsec();
        if (
            current.vsx_lookup
            && ["OK", "NOT_FOUND", "ERROR"].includes(String(current.vsx_lookup.status || ""))
            && Number(current.vsx_lookup.search_radius_arcsec) === requestedRadiusArcsec
        ) {
            return;
        }
        updateRowsForTic(ticId, (item) => ({
            ...item,
            vsx_lookup: {
                status: "PENDING",
                reason: `Lookup VSX in corso (r=${requestedRadiusArcsec.toFixed(1)}")...`,
                tic_id: ticId,
                search_radius_arcsec: requestedRadiusArcsec,
            },
        }));
        renderRows();
        const rowBefore = currentDetailRow();
        if (rowBefore && rowBefore.tic_id === ticId) setDetail(rowBefore);
        try {
            const payload = await fetchJson(
                `${apiBase()}/vsx-lookup?tic_id=${encodeURIComponent(String(ticId))}&radius_arcsec=${encodeURIComponent(String(requestedRadiusArcsec))}`
            );
            updateRowsForTic(ticId, (item) => ({ ...item, vsx_lookup: payload || { status: "UNKNOWN" } }));
        } catch (err) {
            updateRowsForTic(ticId, (item) => ({
                ...item,
                vsx_lookup: {
                    status: "ERROR",
                    reason: err instanceof Error ? err.message : String(err),
                    tic_id: ticId,
                    search_radius_arcsec: requestedRadiusArcsec,
                },
            }));
        }
        renderRows();
        const rowAfter = currentDetailRow();
        if (rowAfter && rowAfter.tic_id === ticId) setDetail(rowAfter);
    }

    async function runFast() {
        if (state.loading) return;
        state.loading = true;
        ui.runBtn.disabled = true;
        if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = true;
        setError(null);
        setStatus("Caricamento TCE FAST in corso...");
        try {
            const payload = await fetchJson(`${apiBase()}/tce?${buildFastQuery().toString()}`);
            state.items = (Array.isArray(payload.items) ? payload.items : []).map((item) => ({
                ...item,
                user_state: loadUserTceState(item.sector, item.tce_id),
                user_note: loadTceNote(item.sector, item.tce_id),
            }));
            setKpis(payload);
            renderRows();
            selectRowByTic(null, null);
            if (!applyInitialSelectionIfPossible()) {
                setStatus(`Caricati ${payload.count || 0} TCE. Ranking=${payload.ranking_version || "base_v1"}.`);
            }
        } catch (err) {
            state.items = [];
            renderRows();
            setKpis(null);
            setError(err instanceof Error ? err.message : String(err));
            setStatus("Errore endpoint FAST.");
        } finally {
            state.loading = false;
            ui.runBtn.disabled = false;
            if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = false;
        }
    }

    async function fetchStatusForTic(ticId) {
        const row = state.items.find((x) => x.tic_id === ticId);
        if (!row) return;
        if (!row.status_available_unknown) return;
        try {
            const payload = await fetchJson(`${apiBase()}/tce/${encodeURIComponent(ticId)}/status`);
            state.items = state.items.map((item) =>
                item.tic_id === ticId
                    ? {
                        ...item,
                        status: payload.status || "UNKNOWN",
                        status_reason: payload.reason || "",
                        status_available_unknown: false,
                    }
                    : item
            );
        } catch (err) {
            state.items = state.items.map((item) =>
                item.tic_id === ticId
                    ? {
                        ...item,
                        status: "UNKNOWN",
                        status_reason: err instanceof Error ? err.message : String(err),
                        status_available_unknown: false,
                    }
                    : item
            );
        }
    }

    async function fetchDvForTic(ticId, preferredTceId) {
        const rowIndex = state.items.findIndex((x) => x.tic_id === ticId);
        if (rowIndex < 0) return;
        const row = state.items[rowIndex];
        const anyRowSameTic = state.items.find((x) => x.tic_id === ticId && x.dv_products !== null);
        if (anyRowSameTic) {
            selectRowByTic(ticId, preferredTceId);
            return;
        }
        setStatus(`Recupero DV on-demand per TIC ${ticId}...`);
        try {
            const payload = await fetchJson(`${apiBase()}/tce/${encodeURIComponent(ticId)}/dv-products`);
            updateRowsForTic(ticId, (item) => ({
                ...item,
                dv_products: Array.isArray(payload.products) ? payload.products : [],
                dv_status: payload.dv_status || (Array.isArray(payload.products) && payload.products.length ? "READY" : "UNAVAILABLE"),
                dv_error: payload.dv_error || null,
                dv_error_type: payload.dv_error_type || null,
                dv_available_unknown: false,
            }));
            renderRows();
            selectRowByTic(ticId, preferredTceId);
            setStatus(`DV on-demand completato per TIC ${ticId}.`);
        } catch (err) {
            updateRowsForTic(ticId, (item) => ({
                ...item,
                dv_products: [],
                dv_status: "RETRY",
                dv_error: err instanceof Error ? err.message : String(err),
                dv_error_type: "fetch_error",
                dv_available_unknown: false,
            }));
            renderRows();
            selectRowByTic(ticId, preferredTceId);
            setStatus(`DV non disponibile temporaneamente per TIC ${ticId}.`);
        }
    }

    async function fetchVariabilityForTic(ticId) {
        if (!ticId) return;
        setStatus(`Recupero variabilita' Gaia/VSX per TIC ${ticId}...`);
        await Promise.all([
            ensureGaiaLookupForTic(ticId),
            ensureVsxLookupForTic(ticId),
        ]);
        renderRows();
        if (state.selectedTic === ticId) selectRowByTic(ticId, state.selectedTceId);
        setStatus(`Variabilita' aggiornata per TIC ${ticId}.`);
    }

    async function fetchStatusTopN() {
        if (state.batchLoading) return;
        const topN = Math.max(1, Number(ui.topNInput.value || 20));
        const tics = topUniqueTicsByPredicate(topN, (row) => row.status_available_unknown);
        if (!tics.length) {
            setStatus("Nessun status TAP da aggiornare nei Top N.");
            return;
        }
        state.batchLoading = true;
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = true;
        ui.fetchTopStatusBtn.disabled = true;
        ui.fetchTopDvBtn.disabled = true;
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = true;
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = true;
        if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = true;
        setError(null);
        setStatus(`Recupero status TAP per top ${tics.length} TIC...`);
        try {
            const payload = await fetchJson(`${apiBase()}/status/batch`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ tics }),
            });
            const byTic = new Map((payload.results || []).map((r) => [r.tic_id, r]));
            state.items = state.items.map((row) => {
                const hit = byTic.get(row.tic_id);
                if (!hit) return row;
                return {
                    ...row,
                    status: hit.status || "UNKNOWN",
                    status_reason: hit.reason || "",
                    status_available_unknown: false,
                };
            });
            renderRows();
            if (state.selectedTic) selectRowByTic(state.selectedTic, state.selectedTceId);
            setStatus(`Status TAP aggiornato per ${payload.results ? payload.results.length : 0} TIC.`);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
            setStatus("Errore batch status TAP.");
        } finally {
            state.batchLoading = false;
            if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = false;
            ui.fetchTopStatusBtn.disabled = false;
            ui.fetchTopDvBtn.disabled = false;
            if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = false;
            if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = false;
            if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = false;
        }
    }

    async function fetchVariabilityTopN() {
        if (state.batchLoading) return;
        const topN = Math.max(1, Number(ui.topNInput.value || 20));
        const tics = topUniqueTicsByPredicate(topN, needsVariabilityLookup);
        if (!tics.length) {
            setStatus("Nessuna variabilita' da calcolare nei Top N.");
            return;
        }
        state.batchLoading = true;
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = true;
        ui.fetchTopStatusBtn.disabled = true;
        ui.fetchTopDvBtn.disabled = true;
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = true;
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = true;
        if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = true;
        setError(null);
        setStatus(`Recupero variabilita' Gaia/VSX per top ${tics.length} TIC...`);
        try {
            for (const ticId of tics) {
                await Promise.all([
                    ensureGaiaLookupForTic(ticId),
                    ensureVsxLookupForTic(ticId),
                ]);
            }
            renderRows();
            if (state.selectedTic) selectRowByTic(state.selectedTic, state.selectedTceId);
            setStatus(`Variabilita' aggiornata per ${tics.length} TIC.`);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
            setStatus("Errore batch variabilita'.");
        } finally {
            state.batchLoading = false;
            if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = false;
            ui.fetchTopStatusBtn.disabled = false;
            ui.fetchTopDvBtn.disabled = false;
            if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = false;
            if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = false;
            if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = false;
        }
    }

    async function fetchDvTopN() {
        if (state.batchLoading) return;
        const topN = Math.max(1, Number(ui.topNInput.value || 20));
        const tics = topUniqueTicsByPredicate(topN, (row) => !hasDvLoaded(row));
        if (!tics.length) {
            setStatus("Nessun DV da recuperare nei Top N.");
            return;
        }
        state.batchLoading = true;
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = true;
        ui.fetchTopStatusBtn.disabled = true;
        ui.fetchTopDvBtn.disabled = true;
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = true;
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = true;
        if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = true;
        setError(null);
        setStatus(`Recupero DV batch per top ${tics.length} TIC...`);
        try {
            const payload = await fetchJson(`${apiBase()}/dv-products/batch`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ tics }),
            });
            const byTic = new Map((payload.results || []).map((r) => [r.tic_id, r]));
            state.items = state.items.map((row) => {
                const hit = byTic.get(row.tic_id);
                if (!hit) return row;
                return {
                    ...row,
                    dv_products: hit.products || [],
                    dv_status: hit.dv_status || ((hit.products || []).length ? "READY" : "UNAVAILABLE"),
                    dv_error: hit.dv_error || null,
                    dv_error_type: hit.dv_error_type || null,
                    dv_available_unknown: false,
                };
            });
            renderRows();
            if (state.selectedTic) selectRowByTic(state.selectedTic, state.selectedTceId);
            setStatus(`DV batch completato per ${payload.results ? payload.results.length : 0} TIC.`);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
            setStatus("Errore batch DV.");
        } finally {
            state.batchLoading = false;
            if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = false;
            ui.fetchTopStatusBtn.disabled = false;
            ui.fetchTopDvBtn.disabled = false;
            if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = false;
            if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = false;
            if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = false;
        }
    }

    async function fetchSelectedVariability() {
        if (state.loading || state.batchLoading) return;
        const ticId = state.selectedTic;
        if (!ticId) {
            setStatus("Seleziona una riga per calcolare la variabilita'.");
            return;
        }
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = true;
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = true;
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = true;
        ui.fetchTopStatusBtn.disabled = true;
        ui.fetchTopDvBtn.disabled = true;
        try {
            await fetchVariabilityForTic(ticId);
        } finally {
            if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = false;
            if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = false;
            if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = false;
            ui.fetchTopStatusBtn.disabled = false;
            ui.fetchTopDvBtn.disabled = false;
        }
    }

    async function fetchSelectedDv() {
        if (state.loading || state.batchLoading) return;
        const ticId = state.selectedTic;
        if (!ticId) {
            setStatus("Seleziona una riga per recuperare i DV.");
            return;
        }
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = true;
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = true;
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = true;
        ui.fetchTopStatusBtn.disabled = true;
        ui.fetchTopDvBtn.disabled = true;
        try {
            await fetchDvForTic(ticId, state.selectedTceId);
        } finally {
            if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.disabled = false;
            if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.disabled = false;
            if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.disabled = false;
            ui.fetchTopStatusBtn.disabled = false;
            ui.fetchTopDvBtn.disabled = false;
        }
    }

    async function clearRemoteCache() {
        if (state.loading || state.batchLoading) return;
        const confirmed = window.confirm("Azzerare la cache remota del modulo TESS TCE per DV, status e Gaia?");
        if (!confirmed) return;
        if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = true;
        if (ui.runBtn) ui.runBtn.disabled = true;
        setError(null);
        setStatus("Azzeramento cache in corso...");
        try {
            const payload = await fetchJson(`${apiBase()}/cache/clear`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ namespaces: ["mast_dv_products", "exo_status", "gaia_lookup", "vsx_lookup", "tic_catalog"] }),
            });
            const totalDeleted = Number(payload.total_deleted || 0);
            const cachePath = payload.cache_path ? ` (${payload.cache_path})` : "";
            setStatus(`Cache azzerata: ${totalDeleted} record rimossi${cachePath}.`);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
            setStatus("Errore durante l'azzeramento cache.");
        } finally {
            if (ui.clearCacheBtn) ui.clearCacheBtn.disabled = false;
            if (ui.runBtn) ui.runBtn.disabled = false;
        }
    }

    function handleTableClick(event) {
        const target = event.target;
        if (!(target instanceof Element)) return;
        if (target.closest("[data-stop-row='1']")) return;
        const rowEl = target.closest("tr[data-tic-id]");
        if (!rowEl) return;
        const ticId = rowEl.getAttribute("data-tic-id");
        const tceId = rowEl.getAttribute("data-tce-id");
        if (!ticId) return;
        selectRowByTic(ticId, tceId || null);
    }

    function handleTableChange(event) {
        const target = event.target;
        if (!(target instanceof HTMLSelectElement)) return;
        if (!target.classList.contains("row-user-state-select")) return;
        event.preventDefault();
        event.stopPropagation();
        const tceId = target.getAttribute("data-tce-id") || "";
        const nextState = String(target.value || "");
        if (!tceId || !USER_TCE_STATES.includes(nextState)) return;
        state.items = state.items.map((row) => {
            if (row.tce_id !== tceId) return row;
            saveUserTceState(row.sector, tceId, nextState);
            return { ...row, user_state: nextState };
        });
        renderRows();
    }

    function applyStateFiltersFromUi() {
        const selected = new Set();
        ui.stateFilterInputs.forEach((el) => {
            if (el instanceof HTMLInputElement && el.checked && USER_TCE_STATES.includes(el.value)) {
                selected.add(el.value);
            }
        });
        state.selectedUserStates = selected;
        const allSelected = USER_TCE_STATES.every((s) => selected.has(s));
        if (ui.filterAllStatesInput) ui.filterAllStatesInput.checked = allSelected;
        renderRows();
    }

    function applyVariabilityFiltersFromUi() {
        const selected = new Set();
        ui.variabilityFilterInputs.forEach((el) => {
            if (el instanceof HTMLInputElement && el.checked && VARIABILITY_FILTER_VALUES.includes(el.value)) {
                selected.add(el.value);
            }
        });
        state.selectedVariabilityStates = selected;
        const allSelected = VARIABILITY_FILTER_VALUES.every((s) => selected.has(s));
        if (ui.filterAllVariabilityInput) ui.filterAllVariabilityInput.checked = allSelected;
        renderRows();
    }

    function handleStateFilterChange(event) {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) return;
        if (target === ui.filterAllStatesInput) {
            const checked = !!target.checked;
            ui.stateFilterInputs.forEach((el) => {
                if (el instanceof HTMLInputElement) el.checked = checked;
            });
            applyStateFiltersFromUi();
            return;
        }
        if (target.classList.contains("tceStateFilterInput")) {
            applyStateFiltersFromUi();
        }
        if (target === ui.filterAllVariabilityInput) {
            const checked = !!target.checked;
            ui.variabilityFilterInputs.forEach((el) => {
                if (el instanceof HTMLInputElement) el.checked = checked;
            });
            applyVariabilityFiltersFromUi();
            return;
        }
        if (target.classList.contains("tceVariabilityFilterInput")) {
            applyVariabilityFiltersFromUi();
        }
    }

    function getProjectIndex() {
        try {
            const raw = localStorage.getItem(PROJECT_INDEX_KEY);
            const arr = raw ? JSON.parse(raw) : [];
            return Array.isArray(arr) ? arr.filter((x) => typeof x === "string" && x.trim() !== "") : [];
        } catch (_) {
            return [];
        }
    }

    function saveProjectIndex(names) {
        try {
            localStorage.setItem(PROJECT_INDEX_KEY, JSON.stringify(names));
        } catch (_) {
            // ignore storage errors
        }
    }

    function refreshProjectOptions(preferredName) {
        if (!ui.projectSelect) return;
        const names = getProjectIndex().sort((a, b) => a.localeCompare(b, "it"));
        const selectedName = preferredName || ui.projectSelect.value || "";
        ui.projectSelect.innerHTML = `<option value="">-- seleziona --</option>${names
            .map((n) => `<option value="${escapeHtml(n)}"${n === selectedName ? " selected" : ""}>${escapeHtml(n)}</option>`)
            .join("")}`;
    }

    function collectFormState() {
        return {
            sector: ui.sectorInput.value,
            limit: ui.limitInput.value,
            min_mes: ui.minMesInput.value,
            max_depth_pct: ui.maxDepthInput.value,
            min_duration_hr: ui.minDurationInput.value,
            max_duration_hr: ui.maxDurationInput.value,
            sort_by: ui.sortByInput.value,
            gaia_radius_arcsec: ui.gaiaRadiusInput.value,
            use_cdpp: !!ui.useCdppInput.checked,
            include_dv: !!ui.includeDvInput.checked,
            include_status: !!ui.includeStatusInput.checked,
            top_n: ui.topNInput.value,
        };
    }

    function applyFormState(formState) {
        if (!formState) return;
        ui.sectorInput.value = formState.sector ?? ui.sectorInput.value;
        ui.limitInput.value = formState.limit ?? "";
        ui.minMesInput.value = formState.min_mes ?? ui.minMesInput.value;
        ui.maxDepthInput.value = formState.max_depth_pct ?? "";
        ui.minDurationInput.value = formState.min_duration_hr ?? "";
        ui.maxDurationInput.value = formState.max_duration_hr ?? "";
        ui.sortByInput.value = formState.sort_by || "score";
        if (ui.gaiaRadiusInput) ui.gaiaRadiusInput.value = formState.gaia_radius_arcsec ?? ui.gaiaRadiusInput.value;
        ui.useCdppInput.checked = !!formState.use_cdpp;
        ui.includeDvInput.checked = !!formState.include_dv;
        ui.includeStatusInput.checked = !!formState.include_status;
        ui.topNInput.value = formState.top_n ?? ui.topNInput.value;
    }

    function collectAnnotationMap() {
        const map = {};
        for (const row of state.items) {
            if (!row || !row.tce_id) continue;
            map[row.tce_id] = {
                sector: row.sector,
                user_state: USER_TCE_STATES.includes(row.user_state) ? row.user_state : "IN_ANALISI",
                user_note: String(row.user_note || "").slice(0, NOTE_MAX_LEN),
            };
        }
        return map;
    }

    function applyAnnotationMap(map) {
        if (!map || typeof map !== "object") return;
        state.items = state.items.map((row) => {
            const hit = map[row.tce_id];
            if (!hit) return row;
            const nextState = USER_TCE_STATES.includes(hit.user_state) ? hit.user_state : row.user_state;
            const nextNote = String(hit.user_note || "").slice(0, NOTE_MAX_LEN);
            saveUserTceState(row.sector, row.tce_id, nextState);
            saveTceNote(row.sector, row.tce_id, nextNote);
            return {
                ...row,
                user_state: nextState,
                user_note: nextNote,
            };
        });
    }

    async function saveCurrentProject() {
        const rawName = (ui.projectNameInput && ui.projectNameInput.value) || (ui.projectSelect && ui.projectSelect.value) || "";
        const name = String(rawName).trim();
        if (!name) {
            setStatus("Inserisci un nome progetto prima del salvataggio.");
            return;
        }
        const snapshot = {
            version: 1,
            saved_at: new Date().toISOString(),
            name,
            form: collectFormState(),
            filters: {
                selected_user_states: Array.from(state.selectedUserStates),
                selected_variability_states: Array.from(state.selectedVariabilityStates),
            },
            annotations: collectAnnotationMap(),
            selected: {
                tic_id: state.selectedTic,
                tce_id: state.selectedTceId,
            },
        };
        try {
            localStorage.setItem(storageKeyForProject(name), JSON.stringify(snapshot));
            const names = new Set(getProjectIndex());
            names.add(name);
            saveProjectIndex(Array.from(names));
            if (ui.projectNameInput) ui.projectNameInput.value = name;
            refreshProjectOptions(name);
            setStatus(`Progetto "${name}" salvato.`);
        } catch (err) {
            setError(`Salvataggio progetto fallito: ${err instanceof Error ? err.message : String(err)}`);
        }
    }

    async function loadSelectedProject() {
        const name = (ui.projectSelect && ui.projectSelect.value) ? ui.projectSelect.value : "";
        if (!name) {
            setStatus("Seleziona un progetto da ripristinare.");
            return;
        }
        let snapshot = null;
        try {
            const raw = localStorage.getItem(storageKeyForProject(name));
            snapshot = raw ? JSON.parse(raw) : null;
        } catch (err) {
            setError(`Lettura progetto fallita: ${err instanceof Error ? err.message : String(err)}`);
            return;
        }
        if (!snapshot || typeof snapshot !== "object") {
            setStatus(`Progetto "${name}" non trovato.`);
            return;
        }
        applyFormState(snapshot.form || {});
        const selectedStates = Array.isArray(snapshot.filters && snapshot.filters.selected_user_states)
            ? snapshot.filters.selected_user_states.filter((x) => USER_TCE_STATES.includes(x))
            : USER_TCE_STATES;
        const selectedVariabilityStates = Array.isArray(snapshot.filters && snapshot.filters.selected_variability_states)
            ? snapshot.filters.selected_variability_states.filter((x) => VARIABILITY_FILTER_VALUES.includes(x))
            : VARIABILITY_FILTER_VALUES;
        ui.stateFilterInputs.forEach((el) => {
            if (!(el instanceof HTMLInputElement)) return;
            el.checked = selectedStates.includes(el.value);
        });
        ui.variabilityFilterInputs.forEach((el) => {
            if (!(el instanceof HTMLInputElement)) return;
            el.checked = selectedVariabilityStates.includes(el.value);
        });
        applyStateFiltersFromUi();
        applyVariabilityFiltersFromUi();
        await runFast();
        applyAnnotationMap(snapshot.annotations || {});
        renderRows();
        const selected = snapshot.selected || {};
        selectRowByTic(selected.tic_id || null, selected.tce_id || null);
        if (ui.projectNameInput) ui.projectNameInput.value = name;
        if (ui.projectSelect) ui.projectSelect.value = name;
        setStatus(`Progetto "${name}" ripristinato.`);
    }

    function deleteSelectedProject() {
        const name = (ui.projectSelect && ui.projectSelect.value) ? ui.projectSelect.value : "";
        if (!name) {
            setStatus("Seleziona un progetto da eliminare.");
            return;
        }
        try {
            localStorage.removeItem(storageKeyForProject(name));
            const names = getProjectIndex().filter((x) => x !== name);
            saveProjectIndex(names);
            refreshProjectOptions("");
            if (ui.projectNameInput && ui.projectNameInput.value === name) ui.projectNameInput.value = "";
            setStatus(`Progetto "${name}" eliminato.`);
        } catch (err) {
            setError(`Eliminazione progetto fallita: ${err instanceof Error ? err.message : String(err)}`);
        }
    }

    function handleXmlViewerClick(event) {
        const target = event.target;
        if (!(target instanceof Element)) return;
        const link = target.closest("a[data-xml-viewer='1']");
        if (!link) return;
        event.preventDefault();
        event.stopPropagation();
        const xmlUrl = link.getAttribute("data-xml-url") || link.getAttribute("href") || "";
        const filename = link.getAttribute("data-xml-filename") || link.textContent || "XML DV";
        if (!xmlUrl) return;
        void openXmlViewer(xmlUrl, filename);
    }

    function handleDetailClick(event) {
        const target = event.target;
        if (!(target instanceof Element)) return;
        const actionEl = target.closest("[data-dv-action]");
        if (!actionEl) return;
        event.preventDefault();
        event.stopPropagation();
        const action = actionEl.getAttribute("data-dv-action");
        const docKey = actionEl.getAttribute("data-doc-key") || "";
        const ticId = state.selectedTic;
        if (!ticId) return;
        if (action === "toggle-kind") {
            const kind = (actionEl.getAttribute("data-doc-kind") || "").toUpperCase();
            if (!kind) return;
            const currentMap = state.expandedDocKindsByTic[ticId] || {};
            state.expandedDocKindsByTic[ticId] = { ...currentMap, [kind]: !currentMap[kind] };
            const row = currentDetailRow();
            if (row) setDetail(row);
            return;
        }
        if (action === "save-note") {
            const tceId = actionEl.getAttribute("data-note-tce-id") || state.selectedTceId || "";
            if (!tceId) return;
            const textarea = Array.from(ui.dvListWrap.querySelectorAll(".tce-note-input"))
                .find((el) => el.getAttribute("data-note-tce-id") === String(tceId));
            const noteText = textarea instanceof HTMLTextAreaElement ? textarea.value : "";
            state.items = state.items.map((row) => {
                if (row.tce_id !== tceId) return row;
                const saved = saveTceNote(row.sector, tceId, noteText);
                return { ...row, user_note: saved };
            });
            setStatus(`Nota salvata per TCE ${tceId}.`);
            return;
        }
        if (!docKey) return;
        if (action === "select" || action === "preview") {
            void previewDocumentForTic(ticId, docKey);
        }
    }

    function handleDetailInput(event) {
        const target = event.target;
        if (!(target instanceof HTMLTextAreaElement)) return;
        if (!target.classList.contains("tce-note-input")) return;
        const tceId = target.getAttribute("data-note-tce-id") || "";
        if (!tceId) return;
        const normalized = target.value.slice(0, NOTE_MAX_LEN);
        if (normalized !== target.value) target.value = normalized;
        state.items = state.items.map((row) => {
            if (row.tce_id !== tceId) return row;
            return { ...row, user_note: normalized };
        });
        const countEl = target.closest(".tce-note-wrap")?.querySelector(".tce-note-count");
        if (countEl) countEl.textContent = String(normalized.length);
    }

    function bind() {
        if (ui.runBtn) ui.runBtn.addEventListener("click", () => void runFast());
        if (ui.clearCacheBtn) ui.clearCacheBtn.addEventListener("click", () => void clearRemoteCache());
        if (ui.fetchTopVarBtn) ui.fetchTopVarBtn.addEventListener("click", () => void fetchVariabilityTopN());
        if (ui.fetchTopStatusBtn) ui.fetchTopStatusBtn.addEventListener("click", () => void fetchStatusTopN());
        if (ui.fetchTopDvBtn) ui.fetchTopDvBtn.addEventListener("click", () => void fetchDvTopN());
        if (ui.fetchSelectedVarBtn) ui.fetchSelectedVarBtn.addEventListener("click", () => void fetchSelectedVariability());
        if (ui.fetchSelectedDvBtn) ui.fetchSelectedDvBtn.addEventListener("click", () => void fetchSelectedDv());
        if (ui.resultsBody) ui.resultsBody.addEventListener("click", handleTableClick);
        if (ui.resultsBody) ui.resultsBody.addEventListener("change", handleTableChange);
        if (ui.dvListWrap) ui.dvListWrap.addEventListener("click", handleDetailClick);
        if (ui.dvListWrap) ui.dvListWrap.addEventListener("input", handleDetailInput);
        if (ui.filterAllStatesInput) ui.filterAllStatesInput.addEventListener("change", handleStateFilterChange);
        if (ui.filterAllVariabilityInput) ui.filterAllVariabilityInput.addEventListener("change", handleStateFilterChange);
        ui.stateFilterInputs.forEach((el) => el.addEventListener("change", handleStateFilterChange));
        ui.variabilityFilterInputs.forEach((el) => el.addEventListener("change", handleStateFilterChange));
        if (ui.saveProjectBtn) ui.saveProjectBtn.addEventListener("click", () => void saveCurrentProject());
        if (ui.loadProjectBtn) ui.loadProjectBtn.addEventListener("click", () => void loadSelectedProject());
        if (ui.deleteProjectBtn) ui.deleteProjectBtn.addEventListener("click", deleteSelectedProject);
        if (ui.projectSelect) ui.projectSelect.addEventListener("change", () => {
            if (ui.projectNameInput && ui.projectSelect.value) ui.projectNameInput.value = ui.projectSelect.value;
        });
        document.addEventListener("click", handleXmlViewerClick);
    }

    function init() {
        state.initialQuery = normalizedInitialQuery();
        if (ui.kpiApi) ui.kpiApi.textContent = apiBase();
        const summary = integrationSummary(state.initialQuery);
        if (summary) {
            setStatus(`Contesto integrato rilevato: ${summary}.`);
        }
        refreshProjectOptions("");
        applyStateFiltersFromUi();
        applyVariabilityFiltersFromUi();
        bind();
    }

    init();
})();
