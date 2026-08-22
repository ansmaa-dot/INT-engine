Two standing security rules for this repo — do not regress either:

1. Never re-render already-rendered HTML through Jinja2 (no 
   render_template_string() on HTML with user-controlled values already 
   substituted in). This is an SSTI/RCE-class bug. Every HTML-returning 
   function calls render_template() with a source template exactly once.

2. Never let user/config-supplied data reach code execution, raw SQL, or a 
   page's rendered output unsafely — no eval/exec/dynamic scripting, no 
   string-interpolated SQL (validate identifiers, use params), no echoing 
   stored secrets back into forms, no un-sandboxed regex from config. If a 
   task seems to need one of these, that's a sign the safe primitive 
   (whitelisted function, parameterized query, validated identifier) is 
   missing — add that instead.

When you touch code that intersects either rule, confirm it still holds and 
say so in your diff summary.