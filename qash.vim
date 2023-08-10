if exists("b:current_syntax")
    finish
endif

syntax clear

syntax match qashCommand "!open-account\|!import"
syntax match qashKeyword "asset\|expense\|equity\|income\|liability"
syntax match qashNumber "-\?\d\+"
syntax match qashDate "\d\{4}-\d\{2}-\d\{2}"
syntax match qashQuote "\".\+\""
syntax match qashLineComment "//.\+"
syntax match qashTag "#.\+"

syntax region qashRangeComment start="(\*" end="\*)" contains=qashRangeComment

highlight link qashCommand Keyword
highlight link qashKeyword Type
highlight link qashNumber Number
highlight link qashDate Constant
highlight link qashQuote String
highlight link qashLineComment Comment
highlight link qashRangeComment Comment
highlight link qashTag Label

let b:current_syntax = "qash"
