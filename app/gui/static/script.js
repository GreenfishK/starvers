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
    let timerInterval;

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
        console.log("Dropdown changed to repo:", selectedRepo, "fetching new data.");

        // Remove download button if it exists
        if (downloadButton) downloadButton.parentElement.removeChild(downloadButton);

        fetch(`/infos/${selectedRepo}`)
            .then(response => {
                if (!response.ok) throw new Error("Failed to load data");
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    plotContainer.innerHTML = `<p class='has-text-danger'>${data.error}</p>`;
                    trackingInfo.innerHTML = "";
                } else {
                    plotContainer.innerHTML = `
                        <div id="delta-plot" class="plot-box">${data.delta_plot_html}</div>
                        <div id="total-plot" class="plot-box">${data.total_plot_html}</div>
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
    
        // Optional: clear previous query results on repo change
        document.getElementById("result-table").innerHTML = "";
    });

    console.log("Initializing listener for the SPARQL form submission.")
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
