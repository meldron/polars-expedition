import { describe } from "polars-wasm";

export interface Stats {
  len: number;
  null_values: number;
  unique_values: number;
  min: number | null;
  median: number | null;
  mean: number | null;
  max: number | null;
}

function formatDate(date: number | string): string {
  if (typeof date === "number") {
    return new Date(date).toISOString();
  }
  return date;
}

function generateTable(data: Record<string, Stats>, dateCols: string[]) {
  const table = document.getElementById("describeTable");

  if (table === null) {
    return;
  }

  table.innerHTML = "";

  // Create the header row
  const headerRow = document.createElement("tr");
  const headerCell = document.createElement("th");
  headerCell.textContent = "";
  headerRow.appendChild(headerCell);

  Object.keys(data).forEach((key) => {
    const cell = document.createElement("th");
    cell.textContent = key;
    headerRow.appendChild(cell);
  });

  table.appendChild(headerRow);

  const metrics: (keyof Stats)[] = [
    "len",
    "null_values",
    "unique_values",
    "min",
    "median",
    "mean",
    "max",
  ];

  const s_metrics: (keyof Stats)[] = ["min", "median", "mean", "max"];

  metrics.forEach((metric) => {
    function formatValue(v: number | null, column: string): string {
      if (v === null) {
        return "␀";
      }

      if (!s_metrics.includes(metric) || !dateCols.includes(column)) {
        return v.toFixed(2);
      }

      return formatDate(v);
    }

    const row = document.createElement("tr");
    const metricCell = document.createElement("td");
    metricCell.textContent = metric;
    row.appendChild(metricCell);

    Object.entries(data).forEach(([column, value]) => {
      const cell = document.createElement("td");
      const metricValue = value[metric];
      cell.textContent = formatValue(metricValue, column);
      row.appendChild(cell);
    });

    table.appendChild(row);
  });
}

function readFile(file: File): void {
  const reader = new FileReader();

  reader.onload = () => {
    const describeDiv = document.getElementById("describe");
    if (describeDiv && reader.result) {
      let description: string | null = null;
      let description_text: string | null = null;

      const dateCols = {
        date: "%Y-%m-%d",
        event_time_date: "%Y-%m-%dT%H:%M:%S%.f",
      };

      try {
        description = describe(reader.result.toString(), dateCols);

        generateTable(JSON.parse(description), Object.keys(dateCols));
        description_text = JSON.stringify(JSON.parse(description), null, 2);
      } catch (err: any) {
        description_text = `Error reading csv: ${err}`;
      }

      describeDiv.textContent = description_text;
    }
  };

  reader.readAsText(file);
}

const fileInput = document.getElementById("fileInput") as HTMLInputElement;

fileInput.addEventListener("change", (event) => {
  const describeDiv = document.getElementById("describe");
  if (describeDiv) {
    describeDiv.textContent = "Loading...";
  }

  const table = document.getElementById("describeTable");
  if (table) {
    table.innerHTML = "";
  }

  const target = event.target as HTMLInputElement;
  const files = target.files;
  if (files && files[0] !== undefined) {
    readFile(files[0]);
  }
});
