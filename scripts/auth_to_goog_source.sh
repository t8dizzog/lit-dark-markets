eval 'set +o history' 2>/dev/null || setopt HIST_IGNORE_SPACE 2>/dev/null
 touch ~/.gitcookies
 chmod 0600 ~/.gitcookies

 git config --global http.cookiefile ~/.gitcookies

 tr , \\t <<\__END__ >>~/.gitcookies
partner-code.googlesource.com,FALSE,/,TRUE,2147483647,o,git-matait.google.com=1//01Kitin-4Pf8YCgYIARAAGAESNwF-L9Irqxo8yrEj4WOXjGtP6a1qtW6_v1_YgbTM7GlXwTmofetkrfw5wCnLREiR0p2MuVaID2g
partner-code-review.googlesource.com,FALSE,/,TRUE,2147483647,o,git-matait.google.com=1//01Kitin-4Pf8YCgYIARAAGAESNwF-L9Irqxo8yrEj4WOXjGtP6a1qtW6_v1_YgbTM7GlXwTmofetkrfw5wCnLREiR0p2MuVaID2g
__END__
eval 'set -o history' 2>/dev/null || unsetopt HIST_IGNORE_SPACE 2>/dev/null
