open Lwt.Infix

module Store = struct
  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let select_account_transactions =
      (string ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE a.name = ?
ORDER BY t.created_at, t.id, p.id
|}

    let select_transactions =
      (unit ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
ORDER BY t.created_at, t.id, p.id
|}

    let select_accounts =
      (unit ->* string) {|SELECT name FROM accounts ORDER BY name|}
  end

  let decode_transactions fold arg =
    let aux
        (tid, created_at, (t_narration, p_narration, a_name, (amount, balance)))
        (cur, acc) =
      let posting =
        Model.(
          make_posting ~narration:p_narration
            ~account:(account_of_string a_name) ~amount ~balance ())
      in
      let tx =
        Model.(
          make_transaction
            ~date:(date_of_string created_at)
            ~narration:t_narration ~postings:[ posting ] ())
      in
      match cur with
      | None -> (Some (tid, tx), acc)
      | Some (tid', cur) when tid = tid' ->
          let cur = Model.{ cur with postings = posting :: cur.postings } in
          (Some (tid, cur), acc)
      | Some (_, cur) -> (Some (tid, tx), cur :: acc)
    in
    match%lwt fold aux arg (None, []) with
    | Error _ -> failwith "failed to decode transactions from db"
    | Ok (None, acc) -> Lwt.return acc
    | Ok (Some (_, cur), acc) -> Lwt.return (cur :: acc)

  let select_transactions (module Db : Caqti_lwt.CONNECTION) =
    decode_transactions (Db.fold Q.select_transactions) ()

  let select_account_transactions (module Db : Caqti_lwt.CONNECTION) account =
    decode_transactions (Db.fold Q.select_account_transactions) account

  let select_accounts (module Db : Caqti_lwt.CONNECTION) =
    match%lwt Db.fold Q.select_accounts List.cons () [] with
    | Error _ -> failwith "failed to decode accounts from db"
    | Ok l -> Lwt.return l
end

let jingoo_model_of_transactions rows =
  let open Jingoo in
  let open Jg_types in
  let open Model in
  let string_of_amount i =
    let rec aux s =
      if String.length s <= 3 then s
      else
        aux (String.sub s 0 (String.length s - 3))
        ^ ","
        ^ String.sub s (String.length s - 3) 3
    in
    aux (string_of_int i)
  in
  rows
  |> List.map @@ fun tx ->
     Tobj
       [
         ("date", Tstr (string_of_date tx.date));
         ("narration", Tstr tx.narration);
         ( "postings",
           Tlist
             (tx.postings
             |> List.map (fun (p : posting) ->
                    Tobj
                      [
                        ("narration", Tstr p.narration);
                        ("account", Tstr (string_of_account p.account));
                        ("amount", Tint p.amount);
                        ("abs_amount_s", Tstr (string_of_amount (abs p.amount)));
                        ("balance", Tint p.balance);
                        ("balance_s", Tstr (string_of_amount p.balance));
                      ])) );
       ]

let serve in_filename =
  let m = Loader.load_file in_filename in
  let%lwt con = Sql_writer.dump "sqlite3::memory:" m in

  let%lwt model_gl =
    Store.select_transactions con >|= jingoo_model_of_transactions
  in

  let%lwt accounts = Store.select_accounts con in
  let%lwt model_accounts =
    Lwt_list.map_s
      (fun account ->
        Store.select_account_transactions con account
        >|= jingoo_model_of_transactions
        >|= fun model -> (account, Jingoo.Jg_types.Tlist model))
      accounts
  in
  let models =
    Jingoo.Jg_types.[ ("gl", Tlist model_gl); ("account", Tobj model_accounts) ]
  in
  Jingoo.Jg_template.from_string ~models
    {|
{%- macro transaction_table (rows) -%}
<details>
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
</details>
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
</style>
<title>Qash</title>
</head>
<body>
<h1>Qash</h1>
{%- for account, rows in account -%}
<a href="#{{ account }}">{{ account }}</a>
{% endfor -%}
{%- for account, rows in account -%}
<h2 id="{{ account }}">{{ account }}</h2>
{{ transaction_table (rows) }}
{% endfor -%}
<h2>総勘定元帳</h2>
{{ transaction_table (gl) }}
</body>
</html>
|}
  |> print_string;
  print_newline ();

  Lwt.return_unit
