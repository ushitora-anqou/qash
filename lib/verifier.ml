open Util

module Store = struct
  module Q = struct
    let select_unbalance_transactions =
      {|
SELECT t.id, t.created_at, t.narration, sum(p.amount)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
GROUP BY t.id
HAVING sum(p.amount) <> 0
|}
  end

  open Datastore

  let select_unbalance_transactions (con : connection) =
    query (prepare con Q.select_unbalance_transactions) []
    |> Result.get_ok
    |> List.map (function
         | [ Int id; Text date; Text narration; Int sum ] ->
             (id, date, narration, sum)
         | _ -> assert false)
end

let verify_balanced_transactions con =
  let unbalanced = Store.select_unbalance_transactions con in
  if unbalanced <> [] then
    Error
      (unbalanced
      |> List.map (fun (_, date, narration, sum) ->
             Printf.sprintf "\t%s %s %d" date narration sum)
      |> String.concat "\n"
      |> Printf.sprintf "Unbalanced transactions:\n%s")
  else Ok ()

let verify_notes (con : Datastore.connection) notes =
  try
    notes
    |> List.iter (fun note ->
           match note with
           | Loader.Show sql -> (
               match Datastore.(query (prepare con sql) []) with
               | Error msg -> failwithf "Error !show: %s\n" msg
               | Ok [ [ Text s ] ] -> Printf.printf "%s\n" s
               | _ -> failwith "Error !show: invalid result")
           | Assert sql -> (
               match Datastore.(query (prepare con sql) []) with
               | Error msg -> failwithf "Error !assert: %s\n" msg
               | Ok [ [ Int s ] ] ->
                   if s = 1 then () else failwithf "Assertion failed: %s" sql
               | _ -> failwith "Error !assert: invalid result"));
    Ok ()
  with Failure s -> Error s

let verify con notes =
  let ( let* ) = Result.bind in
  let* () = verify_balanced_transactions con in
  let* () = verify_notes con notes in
  Ok ()
