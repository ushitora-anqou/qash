open Qash
open Util

let gnucash_to_source transactions_csv_filename =
  with_file transactions_csv_filename @@ fun ic ->
  let csv = Csv.of_channel ic in
  let _ = Csv.next csv (* Skip header row *) in
  let date_re =
    Regex.e {|^([0-9][0-9][0-9][0-9])年([0-9][0-9])月([0-9][0-9])日$|}
  in
  let read_row row =
    let date = List.nth row 0 in
    let transaction_narration = List.nth row 3 in
    let posting_narration = List.nth row 8 in
    let account = List.nth row 9 |> Model.account_of_string in
    let amount = List.nth row 12 |> Model.amount_of_string in
    match Regex.match_ date_re date with
    | [
     [|
       _;
       Some { substr = year; _ };
       Some { substr = month; _ };
       Some { substr = day; _ };
     |];
    ] ->
        `Transaction
          Model.(
            make_transaction
              ~date:
                (make_date ~year:(int_of_string year)
                   ~month:(int_of_string month) ~day:(int_of_string day))
              ~flag:"*" ~narration:transaction_narration
              ~postings:
                [
                  make_posting ~account ~amount ~narration:posting_narration ();
                ]
              ())
    | _ -> `Posting Model.(make_posting ~account ~amount ())
  in
  let cur, acc =
    Csv.fold_left csv ~init:(None, []) ~f:(fun (cur, acc) row ->
        match read_row row with
        | exception e ->
            Printf.eprintf "Error: %s: %s\n" (Printexc.to_string e)
              (String.concat ", " row);
            (cur, acc)
        | `Transaction txn ->
            ( Some txn,
              cur
              |> Option.fold ~none:acc ~some:(fun x ->
                     Model.{ x with postings = List.rev x.postings } :: acc) )
        | `Posting posting -> (
            match cur with
            | None -> failwith "Invalid format; posting without transaction"
            | Some cur ->
                (Some { cur with postings = posting :: cur.postings }, acc)))
  in
  let acc = cur |> Option.fold ~none:acc ~some:(fun x -> x :: acc) in
  let acc =
    acc
    |> List.sort (fun Model.{ date = lhs; _ } Model.{ date = rhs; _ } ->
           compare
             [ lhs.year; lhs.month; lhs.day ]
             [ rhs.year; rhs.month; rhs.day ])
  in
  acc
  |> List.iter (fun txn ->
         Model.string_of_transaction txn |> Printf.printf "%s\n")

let check filename =
  match Parser.parse_file filename with Ok _ -> () | Error s -> failwith s

let () =
  let open Cmdliner in
  Cmd.(
    group
      (info "qash" ~version:"0.1.0" ~doc:"A command-line accounting tool")
      [
        v (info "gnucash-to-source")
          Term.(
            const gnucash_to_source
            $ Arg.(
                required
                & pos 0 (some string) None
                & info ~docv:"TRANSACTIONS-CSV-FILE" []));
        v (info "check")
          Term.(
            const check
            $ Arg.(required & pos 0 (some string) None & info ~docv:"FILE" []));
      ]
    |> eval)
  |> exit
