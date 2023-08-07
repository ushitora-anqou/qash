{%- macro bars_table (canvas_id, rows, stacked) -%}
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
{%- for ent in rows.data -%}
      {
        label: '{{ ent.account }}',
        data: [{{ ent.data | join(', ') }}],
        borderWidth: 1,
        backgroundColor: colors[{{ loop.index0 }} % colors.length],
        stack: '{{ ent.stack }}',
      },
{%- endfor -%}
    ]
  },
  options: {
    scales: {
      x: {
        stacked: {{ stacked }},
      },
      y: {
        beginAtZero: true,
        stacked: {{ stacked }},
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
<script src='https://cdnjs.cloudflare.com/ajax/libs/react/18.2.0/umd/react.production.min.js'></script>
<script src='https://cdnjs.cloudflare.com/ajax/libs/react-dom/18.2.0/umd/react-dom.production.min.js'></script>
<script src='https://cdnjs.cloudflare.com/ajax/libs/react-router-dom/5.3.4/react-router-dom.min.js'></script>
<script src='https://cdnjs.cloudflare.com/ajax/libs/babel-standalone/7.22.9/babel.min.js'></script>
<style>
.app {
  width: 100vw;
  height: 100vh;
  display: flex;
  flex-direction: row;
  flex-grow: 1;
}
.sidebar {
  width: 20%;
  padding: 1em;
}
.content {
  display: flex;
  flex-direction: column;
  flex-grow: 1;
}
.amount {
  text-align: right;
}
.account {
  border: 1px solid black;
  padding: 5px;
  margin: 5px;
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
<div id='root'></div>

<script type='text/babel'>
const Link = ReactRouterDOM.Link;
const Route = ReactRouterDOM.Route;

function PageGL() {
  return <h1>総勘定元帳</h1>;
}
function PageCharts() {
  return <h1>チャート</h1>;
}
{% for account, rows in account -%}
{%- if length(rows) != 0 -%}
function Page{{ loop.index }}() {
  return <h1>{{ account }}</h1>;
}
{% endif -%}
{% endfor -%}

const App = () => (
  <ReactRouterDOM.HashRouter>
    <div class="app">
      <div class="sidebar">
        <div><Link to="/">総勘定元帳</Link></div>
        <div><Link to="/charts">チャート</Link></div>

{% for account, rows in account -%}
{%- if length(rows) != 0 %}
        <Link to="/account/{{ account | replace(':', '_') }}">
          <div class="account">
            <div>{{ account }}</div>
            <div class="amount">{{ rows[0].postings[0].balance_s }} JPY</div>
          </div>
        </Link>
{% endif -%}
{% endfor -%}
      </div>

      <div class="content">
        <Route path="/" exact component={PageGL} />
        <Route path="/charts" exact component={PageCharts} />

{%- for account, rows in account -%}
{%- if length(rows) != 0 -%}
        <Route path="/account/{{ account | replace(':', '_') }}" exact component={Page{{ loop.index }}} />
{%- endif -%}
{% endfor -%}
      </div>
    </div>
  </ReactRouterDOM.HashRouter>
)

ReactDOM.render(<App />, document.querySelector('#root'));
</script>

<!--
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
{{ bars_table ("asset-2023", asset, true) }}
<h2>負債チャート</h2>
{{ bars_table ("liability-2023", liability, true) }}
<h2>収益チャート</h2>
{{ bars_table ("income-2023", income, true) }}
<h2>費用チャート</h2>
{{ bars_table ("expense-2023", expense, true) }}
<h2>キャッシュフロー</h2>
{{ bars_table ("cashflow-2023", cashflow, true) }}
<div>
</div>
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
-->
</body>
</html>
