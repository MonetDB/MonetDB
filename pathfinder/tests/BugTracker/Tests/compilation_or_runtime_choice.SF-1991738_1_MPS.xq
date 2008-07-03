for $run in pf:collection("log.xml")//run
let $successCount := count($run/success/task)
let $failureCount := count($run/failure/task)
let $nooutputCount := if ($run/nooutput/@count) then
exactly-one($run/nooutput/@count) cast as xs:integer else 0 +
count($run/nooutput/task)
let $totalTasks := ($successCount + $failureCount + $nooutputCount) cast as
xs:integer

let $started := $run/started/date/@unixtime cast as xs:integer?
let $prepared := $run/prepared/date/@unixtime cast as xs:integer?
let $executed := $run/executed/date/@unixtime cast as xs:integer?
let $validated := $run/validated/date/@unixtime cast as xs:integer?
let $committed := $run/committed/date/@unixtime cast as xs:integer?

return element { "run" } {
$run/@tool,

if ($prepared > 0) then attribute preparation { $prepared - $started }
else (),
if ($executed > 0) then attribute execution { $executed - $prepared }
else (),
if ($validated > 0) then attribute validation { $validated - $executed }
else (),
if ($committed > 0) then attribute commit { $committed - $validated }
else (),
if ($totalTasks > 0)
then
(if ($prepared > 0) then attribute preparationPerTask { ($prepared -
$started) div $totalTasks } else (),
if ($executed > 0) then attribute executionPerTask { ($executed -
$prepared) div $totalTasks } else (),
if ($validated > 0) then attribute vaalidationPerTask { ($validated -
$executed) div $totalTasks } else (),
if ($committed > 0) then attribute commitPerTask { ($committed -
$validated) div $totalTasks } else ())
else
(),
attribute allowedExecution {exactly-one($run/allowedExecutionTime) *
1000},
attribute successCount {$successCount},
attribute failureCount {$failureCount},
attribute nooutputCount {$nooutputCount},
attribute totalTasks {$totalTasks}
}
