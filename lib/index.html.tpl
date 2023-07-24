{%- macro bars_table (canvas_id, rows) -%}
<div>
<canvas id="{{ canvas_id }}" width=1000 height=600></canvas>
<script>
new Chart(document.getElementById('{{ canvas_id }}'), {
  type: 'bar',
  data: {
    labels: [
{%- for label in rows.labels -%}
      '{{ label }}',
{%- endfor -%}
    ],
    datasets: [
{%- for account, data in rows.data -%}
      {
        label: '{{ account }}',
        data: [{{ data | join(', ') }}],
        borderWidth: 1,
        backgroundColor: colors[{{ loop.index0 }} % colors.length],
      },
{%- endfor -%}
    ]
  },
  options: {
    scales: {
      x: {
        stacked: true,
      },
      y: {
        beginAtZero: true,
        stacked: true
      }
    }
  }
});
</script>
</div>
{%- endmacro -%}
{%- macro transaction_table (rows) -%}
<table class="transactions">
<thead>
<tr><td class="col-date">日付</td><td class="col-narration">説明</td><td class="col-account">勘定科目</td><td class="col-debit">借方</td><td class="col-credit">貸方</td><td class="col-balance">貸借残高</td></tr>
</thead>
<tbody>
{%- for tx in rows -%}
{%- for p in tx.postings -%}
<tr>
{%- if loop.first -%}
<td class="col-date">{{ tx.date }}</td><td class="col-narration">{{ tx.narration }}</td>
{%- else -%}
<td class="col-date"></td><td class="col-narration"></td>
{%- endif -%}
{%- if p.amount < 0 -%}
<td class="col-account">{{ p.account }}</td><td class="col-debit"></td><td class="col-credit">{{ p.abs_amount_s }}</td>
{%- else -%}
<td class="col-account">{{ p.account }}</td><td class="col-debit">{{ p.abs_amount_s }}</td><td class="col-credit"></td>
{%- endif -%}
<td class="col-balance">{{ p.balance_s }}</td>
</tr>
{%- endfor -%}
{% endfor -%}
</tbody>
</table>
{%- endmacro -%}

<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<style>
* {
  max-width: 95%;
  margin: 20px auto;
}
table.transactions .col-date {
  width: 10vw;
}
table.transactions .col-narration {
  width: 50vw;
}
table.transactions .col-account {
  width: 10vw;
  text-align: right;
  white-space: nowrap;
  overflow: auto;
}
table.transactions .col-debit {
  width: 10vw;
  text-align: right;
}
table.transactions .col-credit {
  width: 10vw;
  text-align: right;
}
table.transactions .col-balance {
  width: 10vw;
  text-align: right;
}
table.transactions td.number {
  text-align: right;
}
table.transactions thead tr, table.transactions thead td {
  background-color: #96b183;
  border: 2px solid black;
  font-weight: bold;
}
table.transactions tbody tr:nth-child(even) {
  background-color: #f6ffda;
}
table.transactions tbody tr:nth-child(odd) {
  background-color: #bfdeb9;
}
table.transactions, table.transactions th, table.transactions td {
  border: 1px solid black;
  border-collapse: collapse;
  padding: 5px;
  color: #000000;
}
.switchtab > input {
  display: none;
}
.switchtab > label {
  display: inline-block;
  margin: 5px;
  padding: 10px;
  background-color: #f6ffda;
}
.switchtab > input:checked + div {
    display: block;
}
.switchtab > div {
    display: none;
}
</style>
<title>Qash</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const colors = ['#4E79A7', '#A0CBE8', '#F28E2B', '#FFBE7D', '#59A14F', '#8CD17D', '#B6992D', '#F1CE63', '#499894', '#86BCB6', '#E15759', '#FF9D9A', '#79706E', '#BAB0AC', '#D37295', '#FABFD2', '#B07AA1', '#D4A6C8', '#9D7660', '#D7B5A6']

// Handle events
const socket = new WebSocket(`ws://${location.host}/ws`);
socket.addEventListener("message", function(event) {
  if (event.data === 'reload') {
    location.reload();
  }
});
</script>
</head>
<body>
<h1>Qash</h1>

<div class="switchtab">
<label for="gl">総勘定元帳</label>
<label for="charts">チャート</label>
{%- for account, rows in account -%}
{%- if length(rows) != 0 -%}
<label for="{{ account }}">{{ account }}</label>
{%- endif -%}
{% endfor -%}
<input type="radio" id="charts" name="tab" />
<div>
<h2>資産チャート</h2>
{{ bars_table ("asset-2023", asset) }}
<h2>負債チャート</h2>
{{ bars_table ("liability-2023", liability) }}
<h2>収益チャート</h2>
{{ bars_table ("income-2023", income) }}
<h2>費用チャート</h2>
{{ bars_table ("expense-2023", expense) }}
</div>
{%- for account, rows in account -%}
{%- if length(rows) != 0 -%}
<input type="radio" id="{{ account }}" name="tab" />
<div>
<h2 id="{{ account }}">{{ account }}</h2>
{{ transaction_table (rows) }}
</div>
{%- endif -%}
{% endfor -%}
<input type="radio" id="gl" name="tab" checked="checked"/>
<div>
<h2>総勘定元帳</h2>
{{ transaction_table (gl) }}
</div>
</div>
</body>
</html>
