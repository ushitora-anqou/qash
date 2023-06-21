let load_file filename =
  let overwrite_transactions (base : Model.transactions)
      (overlay : Model.transactions) =
    overlay
    |> List.fold_left
         (fun base (ot : Model.transaction) ->
           base
           |> List.map (fun (bt : Model.transaction) ->
                  if
                    bt.date = ot.date
                    && String.starts_with ~prefix:bt.narration ot.narration
                  then ot
                  else bt))
         base
  in
  let rec aux filename =
    let wd = Filename.dirname filename in
    match Parser.parse_file filename with
    | Error e -> failwith e
    | Ok directives ->
        directives
        |> List.fold_left
             (fun (accounts, transactions) -> function
               | Model.OpenAccount row -> (row :: accounts, transactions)
               | Transaction row -> (accounts, row :: transactions)
               | Import { filename; transactions = overlay } ->
                   let filepath = Filename.concat wd filename in
                   let accounts', transactions' = aux filepath in
                   if accounts' <> [] then
                     failwith "imported file should not contain open-account"
                   else
                     ( accounts,
                       transactions
                       @ overwrite_transactions transactions' overlay ))
             ([], [])
  in
  let accounts, transactions = aux filename in
  Model.{ accounts; transactions }
