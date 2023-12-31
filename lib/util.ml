let repeat n v = List.init n (fun _ -> v) |> List.concat
let ( *> ) f g a = f a |> g
let errf fmt = Printf.ksprintf (fun s -> Error s) fmt

let with_file path f =
  if path = "-" then f stdin
  else
    let ic = open_in_bin path in
    Fun.protect ~finally:(fun () -> close_in ic) (fun () -> f ic)

let ignore_lwt f = Lwt.map (fun _ -> ()) f

let iota n =
  let rec aux acc = function 0 -> acc | n -> aux ((n - 1) :: acc) (n - 1) in
  aux [] n

let failwithf fmt = Printf.ksprintf failwith fmt
