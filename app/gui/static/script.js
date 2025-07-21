// app/gui/static/script.js
// This script handles the dynamic behavior of the GUI, including SPARQL query execution,
// repository selection, triple evolution, and result display.

document.addEventListener("DOMContentLoaded", function () {
    console.log("Document loaded.");

    const dropdown = document.getElementById("repo-select");
    const plotContainer = document.getElementById("plot-container");
    const trackingInfo = document.getElementById("tracking-info");
    const sparqlForm = document.getElementById("sparql-form");
    const overlay = document.getElementById("loading-overlay");
    const timerEl = document.getElementById("timer");
    const plotDiv = document.getElementById("total-plot-graph");
    console.log(Plotly.version);


    if (plotDiv) {
        let relayoutTimeout = null;
        let lastYRange = null;

        plotDiv.on("plotly_relayout", (eventdata) => {
            console.log("Plotly relayout event triggered:", eventdata);

            console.log("All trace names in plot:");
            plotDiv.data.forEach((trace, i) => {
                console.log(`Trace ${i}: name='${trace.name}', has x: ${Array.isArray(trace.x)}, has y: ${Array.isArray(trace.y)}`);
            });

            if (!("xaxis.range[0]" in eventdata) || !("xaxis.range[1]" in eventdata)) {
                console.log("Skipped: No xaxis range in eventdata.");
                return;
            }

            const xVals = plotDiv.data[0].x;
            const xStart = eventdata['xaxis.range[0]'];
            const xEnd = eventdata['xaxis.range[1]'];

            console.log("xaxis.range[0]:", Math.floor(xStart));
            console.log("xaxis.range[1]:", Math.ceil(xEnd));
            console.log("trace[0].x.length:", plotDiv.data[0].x.length);
            console.log("trace[1].x.length:", plotDiv.data[1] ? plotDiv.data[1].x.length : "N/A");

            const startIndex = Math.max(0, Math.floor(xStart));
            const endIndex = Math.min(xVals.length - 1, Math.ceil(xEnd));

            console.log("startIndex:", startIndex, "endIndex:", endIndex);

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

            console.log("Visible Y range:", yMin, yMax);
            console.log("Setting yaxis.range:", yRange[0], yRange[1]);

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
        });
    }



    console.log("Initializing SPARQL editor.");
    const editor = CodeMirror.fromTextArea(document.getElementById('sparql-editor'), {
        mode: 'sparql',
        lineNumbers: true
    });
    editor.setSize(null, "420px");

    console.log("Initializing SPARQL-star view.")
    const timestampedEditor = CodeMirror(document.getElementById("timestamped-editor"), {
        value: "",
        mode: "sparql",
        lineNumbers: true,
        theme: "default",
        readOnly: false  
    });
    timestampedEditor.setSize(null, "420px");
    timestampedEditor.getWrapperElement().style.backgroundColor = "#f5f5f5";

    console.log("Initializing listener for the repository dropdown menu.")
    dropdown.addEventListener("change", function () {
        const selectedRepo = dropdown.value;
        const downloadButton = document.getElementById("download-btn");
        console.log("Dropdown changed to repo:", selectedRepo);

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
                if (data.error) {
                    plotContainer.innerHTML = `<p class='has-text-danger'>${data.error}</p>`;
                    trackingInfo.innerHTML = "";
                } else {
                    plotContainer.innerHTML = `
                        <div id="total-plot" class="plot-box">${data.total_plot}</div>
                    `;
                    trackingInfo.innerHTML = `
                        <p><strong>Tracked URL:</strong> <span id="tracked-url">${data.rdf_dataset_url}</span></p>
                        <p><strong>Polling Interval:</strong> <span id="polling-interval">${data.polling_interval}</span> seconds</p>
                    `;
                }
            })
            .catch(error => {
                console.error("Error fetching plot/tracking info:", error);
                plotContainer.innerHTML = "<p class='has-text-danger'>Failed to load plot.</p>";
                trackingInfo.innerHTML = "<p class='has-text-danger'>Failed to load tracking info.</p>";
            });
    
        // Clear previous query results on repo change
        document.getElementById("result-table").innerHTML = "";

        // Clear the timestamped editor
        timestampedEditor.setValue("");
    });

    console.log("Initializing listener for the SPARQL form submission.")
    let timerInterval;
    sparqlForm.addEventListener("submit", function (e) {
        console.log("Form submitted.");
        e.preventDefault(); 
        
        const downloadButton = document.getElementById("download-btn");
        const formData = new FormData(sparqlForm);

        let seconds = 0;
        timerEl.textContent = "0";

        // Remove old download button if it exists
        if (downloadButton) downloadButton.parentElement.removeChild(downloadButton);

        // Show loading overlay and start timer
        overlay.style.display = "flex";
        timerInterval = setInterval(() => {
            seconds += 1;
            timerEl.textContent = seconds;
        }, 1000);

        console.log("Executing query.")
        fetch("/query", {
            method: "POST",
            body: formData
        })
        .then(res => res.json())
        .then(data => {
            clearInterval(timerInterval);
            overlay.style.display = "none";

            const resultTable = document.getElementById("result-table");
            if (data.error) {
                resultTable.innerHTML = `<div class="notification is-danger"><strong>Error:</strong> ${data.error}</div>`;
            } else {
                resultTable.innerHTML = data.html;
                timestampedEditor.setValue(data.timestamped_query || "");
                console.log("Result successfully retrieved.");

                // Check if the download button already exists
                if (!downloadButton) {
                    console.log("Adding download button.");
                    const downloadLink = document.createElement("a");
                    downloadLink.href = "/download";
                    downloadLink.innerHTML = `<button id="download-btn" class="button is-success mt-3">Download CSV</button>`;
                    resultTable.parentElement.appendChild(downloadLink);
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
    });
});

