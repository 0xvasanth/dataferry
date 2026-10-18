#!/usr/bin/env bash
# Builds the guide (docs/) with mdbook and checks the result:
#
# - mdbook printed no warning or error. It only logs a missing
#   {{#include}}, and still exits 0, so the log is what tells.
# - Every relative link and every #anchor in the built HTML resolves, to a
#   page of the book or to a file the build copied.
#
# Links to other sites aren't fetched. Run from anywhere; the book is
# written to docs/book. Needs mdbook (the version docs.yml pins).
set -euo pipefail

docs=$(cd "$(dirname "$0")/.." && pwd)
book=$docs/book
log=$(mktemp)
links=$(mktemp)
trap 'rm -f "$log" "$links"' EXIT

mdbook build "$docs" 2>&1 | tee "$log"
if grep -Eq '(^|[[:space:]])(WARN|ERROR)[[:space:]]' "$log"; then
  echo "FAIL mdbook printed warnings or errors:" >&2
  grep -E '(^|[[:space:]])(WARN|ERROR)[[:space:]]' "$log" >&2
  exit 1
fi

# True when `file` has an element with id `anchor`.
has_anchor() {
  grep -qF "id=\"$2\"" "$1"
}

failed=0
checked=0
# print.html repeats every chapter, toc.html and 404.html are mdbook's own.
while IFS= read -r page; do
  dir=$(dirname "$page")
  grep -oE '(href|src)="[^"]*"' "$page" | sed -E 's/^(href|src)="//; s/"$//' | sort -u >"$links"
  while IFS= read -r link; do
    case $link in
      '' | http://* | https://* | mailto:* | data:* | javascript:*) continue ;;
    esac
    checked=$((checked + 1))
    path=${link%%#*}
    path=${path%%\?*}
    anchor=
    [[ $link == *'#'* ]] && anchor=${link#*#}
    if [[ -z $path ]]; then
      target=$page
    else
      target=$dir/$path
      [[ -d $target ]] && target=$target/index.html
    fi
    if [[ ! -f $target ]]; then
      echo "FAIL ${page#"$book"/}: broken link $link"
      failed=1
    elif [[ -n $anchor && $target == *.html ]] && ! has_anchor "$target" "$anchor"; then
      echo "FAIL ${page#"$book"/}: no anchor #$anchor in ${target#"$book"/}"
      failed=1
    fi
  done <"$links"
done < <(find "$book" -name '*.html' -not -name print.html -not -name toc.html -not -name 404.html | sort)

echo "$checked links checked"
exit "$failed"
