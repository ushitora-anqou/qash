(lang dune 3.8)

(name qash)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name")

(maintainers "Maintainer Name")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name qash)
 (synopsis "A short synopsis")
 (description "A longer description")
 (tags
  (topics "to describe" your project))
 (depends ocaml dune menhir))

(using menhir 2.1)
