type note = Assert of string | Show of string

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
  let rec aux filename : Model.t * note list =
    let wd = Filename.dirname filename in
    match Parser.parse_file filename with
    | Error e -> failwith e
    | Ok directives ->
        directives
        |> List.fold_left
             (fun ((t : Model.t), (notes : note list)) -> function
               | Model.OpenAccount row ->
                   ({ t with accounts = row :: t.accounts }, notes)
               | Transaction row ->
                   ({ t with transactions = row :: t.transactions }, notes)
               | Import { filename; transactions = overlay } ->
                   let filepath = Filename.concat wd filename in
                   let (t' : Model.t), (notes : note list) = aux filepath in
                   if t'.accounts <> [] then
                     failwith "imported file should not contain open-account"
                   else
                     ( {
                         t with
                         transactions =
                           t.transactions
                           @ overwrite_transactions t'.transactions overlay;
                       },
                       notes )
               | Assert s -> (t, Assert s :: notes)
               | Show s -> (t, Show s :: notes))
             ({ accounts = []; transactions = [] }, [])
  in
  aux filename
