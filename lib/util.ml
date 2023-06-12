let repeat n v = List.init n (fun _ -> v) |> List.concat
let ( *> ) f g a = f a |> g
let errf fmt = Printf.ksprintf (fun s -> Error s) fmt

let with_file path f =
  let ic = open_in_bin path in
  Fun.protect ~finally:(fun () -> close_in ic) (fun () -> f ic)