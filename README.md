# shred
Shred big files into little bits

Chunks are aligned on newline boundaries, so no incomplete lines.
Uses as many cores as you've got (unlike `split`), although it's really i/o constrained in the end.

TODO: add support for multiple destination directories so that writes can be spread across them to boost bandwidth.