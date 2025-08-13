// app/gui/static/script.js
// This script handles the dynamic behavior of the GUI, including SPARQL query execution,
// repository selection, triple evolution, and result display.

// Global variables
let activeAggLevel = 'DAY'; 
let currentHaloTraceIndex = null; 
const mediaQuery = window.matchMedia("(max-width: 900px)");

document.addEventListener("DOMContentLoaded", function () {
    console.log("Document loaded.");

    const dropdown = document.getElementById("repo-select");
    const sparqlForm = document.getElementById("sparql-form");
    const plotDiv = document.getElementById("evo-plot");
    const defaultBtn = document.getElementById("day_button")
    
    handleScreenChange(mediaQuery);
    activateTabRight("classes"); 

    if (defaultBtn) {
        changeTimelinePeriod('DAY', defaultBtn);
    }

    if (plotDiv) {
        plotDiv.on("plotly_click", (eventData) => plotly_fetchSnapshotClassHierarchy(eventData, plotDiv, dropdown));
        plotDiv.on("plotly_relayout", (eventData) => plotly_relayout(eventData, plotDiv));
    }

    console.log("Initializing SPARQL editor.");
    const editor = CodeMirror.fromTextArea(document.getElementById('sparql-editor'), {
        mode: 'sparql',
        lineNumbers: true
    });

    console.log("Initializing SPARQL-star view.")
    const timestampedEditor = CodeMirror.fromTextArea(document.getElementById('timestamped-query'), {
        value: "",
        mode: "sparql",
        lineNumbers: true,
        theme: "default",
        readOnly: "nocursor" 
    });

    timestampedEditor.getWrapperElement().style.backgroundColor = "#f5f5f5";

    console.log("Initializing listener for the repository dropdown menu.")
    dropdown.addEventListener("change",  () => repoChange(dropdown, timestampedEditor));

    console.log("Initializing listener for the SPARQL form submission.")
    sparqlForm.addEventListener("submit", (e) => executeQuery(e, sparqlForm, timestampedEditor));
});



/******************************************************************
 * Listeners
 ******************************************************************/
mediaQuery.addEventListener("change", handleScreenChange);

document.querySelectorAll('#mobile-tabs li').forEach(li => {
    li.addEventListener('click', () => {
        const tabId = li.getAttribute('data-tab');
        activateTabMain(tabId);
    });
});


document.querySelectorAll('#right-section-tabs li').forEach(li => {
    li.addEventListener('click', () => {
        const tabId = li.getAttribute('data-tab');
        activateTabRight(tabId);
    });
});


document.querySelectorAll(".agg-button").forEach(button => {
    button.addEventListener("click", function() {
        const level = this.getAttribute("data-agg");
        changeTimelinePeriod(level, this);
    });
});




/******************************************************************
 * Functions
 ******************************************************************/
function handleScreenChange(e) {
    if (e.matches) { 
        // Now <= 900px
        activateTabMain('main');
    } else {
        Array.from(document.getElementsByClassName('content-section'))
        .forEach(section => section.style.display = "flex");    
    }
}

function executeQuery(e, sparqlForm, timestampedEditor) {
    console.log("Form submitted.");
    e.preventDefault(); 
    
    const downloadButton = document.getElementById("download-btn");
    const formData = new FormData(sparqlForm);
    const overlay = document.getElementById("loading-overlay");
    const timerEl = document.getElementById("timer");
    const mainSection = document.getElementById("main-section");


    let seconds = 0;
    timerEl.textContent = "0";

    // Remove old download button if it exists
    if (downloadButton) downloadButton.parentElement.removeChild(downloadButton);

    // Show loading overlay and start timer
    overlay.style.display = "flex";
    let timerInterval;
    timerInterval = setInterval(() => {
        seconds += 1;
        timerEl.textContent = seconds;
    }, 1000);

    console.log("Executing query.")
    // show result container
    document.getElementById("data-section").classList.remove("is-hidden");

    // Scroll main-section to bottom so new data-section is visible
    mainSection.scrollTo({ top: mainSection.scrollHeight, behavior: 'smooth' });


    fetch("/query", {
        method: "POST",
        body: formData
    })
    .then(res => res.json())
    .then(data => {
        // show timestamped query
        timestampedEditor.setValue(data.timestamped_query || "");

        clearInterval(timerInterval);
        overlay.style.display = "none";

        const resultTable = document.getElementById("result-table");
        if (data.error) {
            resultTable.innerHTML = `<div class="notification is-danger"><strong>Error:</strong> ${data.error}</div>`;
        } else {

            resultTable.innerHTML = data.result_set;
            console.log("Result successfully retrieved.");

            // Check if the download button already exists
            if (!downloadButton) {
                console.log("Adding download button.");
                const downloadLink = document.createElement("a");
                downloadLink.href = "/download";
                downloadLink.innerHTML = `<button id="download-btn" class="button is-success mt-3">Download CSV</button>`;
                resultTable.parentElement.appendChild(downloadLink);

                // Scroll main-section to bottom so new data-section is visible
                 mainSection.scrollTo({ top: mainSection.scrollHeight, behavior: 'smooth' });
            }
        
        }
    })
    .catch(err => {
        clearInterval(timerInterval);
        overlay.style.display = "none";

        console.error("Query execution failed:", err);
        document.getElementById("result-table").innerHTML =
            `<div class="notification is-danger"><strong>Error:</strong> Failed to execute query.</div>`;
    });
}

function plotly_relayout(eventData, plotDiv) {
    console.log("Plotly relayout event triggered:", eventData);
    let relayoutTimeout = null;
    let lastYRange = null;

    if (!("xaxis.range[0]" in eventData) || !("xaxis.range[1]" in eventData)) {
        console.log("Skipped: No xaxis range in eventdata.");
        return;
    }

    const xVals = plotDiv.data[0].x;
    const xStart = eventData['xaxis.range[0]'];
    const xEnd = eventData['xaxis.range[1]'];

    const startIndex = Math.max(0, Math.floor(xStart));
    const endIndex = Math.min(xVals.length - 1, Math.ceil(xEnd));

    if (startIndex > endIndex) {
        console.log("No visible data in range.");
        return;
    }

    // Find the "Total Triples" trace
    const traceTotal = plotDiv.data.find(t => t.name === "Total Triples");
    if (!traceTotal) {
        console.warn("Total Triples trace not found, skipping relayout.");
        return;
    }

    // Extract y data as plain array, handling typed arrays
    let yData = traceTotal.y;
    if (!Array.isArray(yData)) {
        if (yData && '_inputArray' in yData) {
            yData = Array.from(yData._inputArray);
        } else {
            console.warn("Total Triples y data unavailable or invalid, skipping relayout.");
            return;
        }
    }

    const visibleTotals = yData.slice(startIndex, endIndex + 1);
    if (visibleTotals.length === 0) {
        console.log("No visible y-values in Total Triples trace, skipping relayout.");
        return;
    }

    const yMin = Math.min(...visibleTotals);
    const yMax = Math.max(...visibleTotals);
    const padding = yMax !== yMin ? (yMax - yMin) * 0.1 : yMax * 0.1 || 1;
    const yRange = [Math.floor(yMin - padding), Math.ceil(yMax + padding)];

    // to show the first bar
    if (xStart < 0.20) {
            yRange[0] = Math.min(0, yRange[0]);
        }

    if (
        lastYRange &&
        Math.abs(lastYRange[0] - yRange[0]) < 1e-3 &&
        Math.abs(lastYRange[1] - yRange[1]) < 1e-3
    ) {
        console.log("y-axis range unchanged, skipping relayout.");
        return;
    }
    lastYRange = yRange;

    if (relayoutTimeout) clearTimeout(relayoutTimeout);
    relayoutTimeout = setTimeout(() => {
        Plotly.relayout(plotDiv, {
            "yaxis.autorange": false,
            "yaxis.range": yRange,
        }).catch((err) => {
            console.warn("Plotly relayout error:", err);
        });
    }, 100); // debounce 100ms
}

function plotly_fetchSnapshotClassHierarchy(eventData, plotDiv, dropdown) {
    if (!eventData || !eventData.points || eventData.points.length === 0) return;

    const point = eventData.points[0];
    const rawTimestamp = point.x;

    const [day, month, year] = rawTimestamp.split(".");
    if (!day || !month || !year) return;

    const formattedTimestamp =
        activeAggLevel === "HOUR"
            ? point.x
            : `${year}-${month}-${day}T23:59:59`;

    document.getElementById("timestamp_input").value = formattedTimestamp;

    fetch(`/statistics?repo=${dropdown.value}&timestamp=${encodeURIComponent(formattedTimestamp)}`, {
        method: "GET",
        headers: {
            "Accept": "application/json" 
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error("Network response was not ok");
        }
        return response.json(); 
    })
    .then(data => {
        const classHierarchy = document.getElementById("right-section-tab-classes");
        if (classHierarchy && data.snapshot_stats) {
            if (data.snapshot_stats.length === 0 && data.snapshot_ts == null) {
                classHierarchy.innerHTML = `
                    <div class="notification is-warning mt-2">
                        <strong>Notice:</strong> No snapshot statistics available for this repository.
                    </div>
                `;                    
            } else {
                classHierarchy.innerHTML = renderSnapshotStats(data.snapshot_stats);
                attachTreeToggleHandlers();
            }
        }
    })
    .catch(error => {
        console.error("Error fetching snapshot statistics:", error);
    });

    // remove previous halo markers
     if (currentHaloTraceIndex !== null) {
        Plotly.deleteTraces(plotDiv, currentHaloTraceIndex);
        currentHaloTraceIndex = null;
    }
    // Add halo marker
    highlightTrace = {
        x: [point.x],
        y: [point.y],
        mode: "markers",
        marker: {
            size: 20,
            color: "rgba(0, 102, 153, 0.4)",  // translucent blue halo
            line: {
                width: 2,
                color: "rgba(0, 102, 153, 1.0)"
            },
            symbol: "circle"
        },
        name: "Selection Halo",
        hoverinfo: "skip",
        showlegend: false
    };

    // Add it as the last trace
    Plotly.addTraces(plotDiv, highlightTrace).then(() => {
        currentHaloTraceIndex = plotDiv.data.length - 1;
    });
}

function repoChange(dropdown, timestampedEditor) {
    const selectedRepo = dropdown.value;
    const downloadButton = document.getElementById("download-btn");
    const plotContainer = document.getElementById("plot-container");
    const trackingInfo = document.getElementById("tracking-infos");
    const defaultBtn = document.getElementById("day_button")

    console.log("Dropdown changed to repo:", selectedRepo);

    //Toggle DAY button
    if (defaultBtn) {
        changeTimelinePeriod('DAY', defaultBtn);
    }

    // Remove download button if it exists
    if (downloadButton) downloadButton.parentElement.removeChild(downloadButton);

    console.log("Fetching plot and tracking info for repo:", selectedRepo);
    fetch(`/infos/${selectedRepo}`)
        .then(response => {
            if (!response.ok) throw new Error("Failed to load data");
            return response.json();
        })
        .then(data => {
            console.log("Data received for repo:", selectedRepo);
            const labels = trackingInfo.querySelectorAll("label");
            if (data.error) {
                plotContainer.innerHTML = `<p class='has-text-danger'>${data.error}</p>`;
                labels[0].lastChild.textContent = "";
                labels[1].lastChild.textContent = "";
                labels[2].lastChild.textContent = "";
            } else {
                const evoPlotObj = JSON.parse(data.evo_plot);
                //Plotly.react("evo-plot", evoPlotObj.data, evoPlotObj.layout);
                
                labels[0].lastChild.textContent = data.rdf_dataset_url;
                labels[1].lastChild.textContent = `${data.polling_interval} seconds`;
                labels[2].lastChild.textContent = data.next_run;
                    
                // clear data section
                document.getElementById("data-section").classList.add("is-hidden")
                document.getElementById("result-table").innerHTML = "";

                // Clear class hierarchy
                console.log("Clearing class hierarchy")
                document.getElementById("right-section-tab-classes").innerHTML = "";

                // Clear the timestamped editor 
                timestampedEditor.setValue("");

                // Show class hierarchy and activate classes tab
                activateTabRight("classes");

                //Reset halo marker index
                currentHaloTraceIndex = null;
            }
        })
        .catch(error => {
            console.error("Error fetching plot/tracking info:", error);
            plotContainer.innerHTML = "<p class='has-text-danger'>Failed to load plot.</p>";
            trackingInfo.innerHTML = "<p class='has-text-danger'>Failed to load tracking info.</p>";
        });


    
};

function changeTimelinePeriod(level, clickedButton) {
    activeAggLevel = level;  // Track current aggregation
    const repo = document.getElementById("repo-select").value;
    const plotContainer = document.getElementById("plot-container");

    // Highlight clicked button, remove is-active from others
    const allAggButtons = document.querySelectorAll(".agg-button");
    allAggButtons.forEach(btn => btn.classList.remove("is-active"));
    clickedButton.classList.add("is-active");

    //Reset halo marker index
    currentHaloTraceIndex = null;

    fetch(`/infos/${repo}?agg=${level}`)
      .then(response => response.json())
      .then(data => {
        if (data.error) {
          plotContainer.innerHTML = `<p class='has-text-danger'>${data.error}</p>`;
          return;
        }
        console.log("Updated plot for aggregation level:", level);
        const evoPlotObj = JSON.parse(data.evo_plot);
        Plotly.react("evo-plot", evoPlotObj.data, evoPlotObj.layout);
      })
      .catch(err => {
        console.error("Failed to fetch new aggregation plot:", err);
        plotContainer.innerHTML = "<p class='has-text-danger'>Failed to update plot.</p>";
      });
  }

function showTooltip(inputField) {
    const timestampTooltip = document.getElementById("timestamp-help");
    if (timestampTooltip && timestampTooltip.title && inputField) {
        const tooltip = document.createElement("div");
        tooltip.id = "timestamp-help-div"
        tooltip.textContent = timestampTooltip.title;

        // Calculate position
        const rect = inputField.getBoundingClientRect();
        tooltip.style.left = `${rect.right + 10}px`; 
        tooltip.style.top = `${rect.top + window.scrollY}px`; 

        document.body.appendChild(tooltip);

        // Animate in
        requestAnimationFrame(() => {
            tooltip.style.opacity = "1";
        });

        // Remove after 5s
        setTimeout(() => {
            tooltip.style.opacity = "0";
            setTimeout(() => {
                tooltip.remove();
            }, 300);
        }, 5000);
    }
}

function renderSnapshotStats(stats) {

    function renderNode(node) {
        const { id, cnt_class_instances, cnt_classes_added, cnt_classes_deleted, children } = node;

        const hasChildren = children && children.length > 0;

        let html = `<section class="tree-node${hasChildren ? ' has-children' : ''}">`;

        if (hasChildren) {
            html += `<span class="expand-btn"></span>`;
        }

        html += `<span class="class-label">${id}</span>`;

        // Wrap info spans in a div for layout control
        html += `<div class="info-row">`;
        html += `<span class="info">Instances: ${cnt_class_instances}</span>`;
        html += `<span class="info">Added: ${cnt_classes_added}</span>`;
        html += `<span class="info">Deleted: ${cnt_classes_deleted}</span>`;
        html += `</div>`;

        if (hasChildren) {
            html += `<section class="children">`;
            children.forEach(child => {
                html += renderNode(child);
            });
            html += `</section>`;
        }

        html += `</section>`;
        return html;
    }


    let fullHtml = `<section id="snapshot-tree">`;
    stats.forEach(node => {
        fullHtml += renderNode(node);
    });
    fullHtml += `</section>`;

    return fullHtml;
}

function attachTreeToggleHandlers() {
    console.log("Attaching expand/collapse handlers...");
    document.querySelectorAll('.tree-node > .expand-btn').forEach(btn => {
        btn.onclick = () => {
            const node = btn.parentElement;
            node.classList.toggle('expanded');
        };
    });
}

/******************************************************************
 * Functions - Tab Functions
 ******************************************************************/
function activateTabMain(tabId) {
    const sections = {
        left: document.getElementById('left-section'),
        main: document.getElementById('main-section'),
        right: document.getElementById('right-section')
    };

    // Remove from all sections
    Object.values(sections).forEach(section => section.style.display = "none");

    // Set the correct one
    if (sections[tabId]) {
        //sections[tabId].classList.add('is-active-tab');
        //sections[tabId].classList.remove('is-hidden');
        sections[tabId].style.display = "flex"
        sections[tabId].style.width = "100%"
    }

    // Update tab UI
    document.querySelectorAll('#mobile-tabs li').forEach(li => li.classList.remove('is-active'));
    const activeLi = document.querySelector(`#mobile-tabs li[data-tab="${tabId}"]`);
    if (activeLi) activeLi.classList.add('is-active');
}


function activateTabRight(tabId) {
    console.log("Switched tab")
    const sections = {
        classes: document.getElementById('right-section-tab-classes'),
        properties: document.getElementById('right-section-tab-properties'),
    };

    // Remove from all sections
    Object.values(sections).forEach(section => {
        section.classList.remove('is-active-tab');
        section.style.display = 'none';
    });

    // Set the correct one
    if (sections[tabId]) {
        sections[tabId].classList.add('is-active-tab');
        sections[tabId].style.display = "block"
    }

    // Update tab UI
    document.querySelectorAll('#right-section-tabs li').forEach(li => li.classList.remove('is-active'));
    const activeLi = document.querySelector(`#right-section-tabs li[data-tab="${tabId}"]`);
    if (activeLi) activeLi.classList.add('is-active');
}


