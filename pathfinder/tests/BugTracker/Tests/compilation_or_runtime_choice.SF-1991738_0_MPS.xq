for $run in pf:collection("log.xml")//run
let $successCount := if ($run/success) then count($run/success/task) else
0
let $failureCount := if ($run/failure) then count($run/failure/task) else
0
let $nooutputCount := if ($run/nooutput) then (if ($run/nooutput/@count)
then zero-or-one($run/nooutput/@count) else 0) + count($run/nooutput/task)
else 0
let $totalTasks := $successCount + $failureCount + $nooutputCount
return <run>
{ $run/@tool }
{ for $v in $run/prepared/date/@unixtime[. > 0] return attribute
preparation {$v - exactly-one($run/started/date/@unixtime)} }
{ for $v in $run/executed/date/@unixtime[. > 0] return attribute
execution {$v - exactly-one($run/prepared/date/@unixtime)} }
{ for $v in $run/validated/date/@unixtime[. > 0] return
attribute validation {$v - exactly-one($run/executed/date/@unixtime)} }
{ for $v in $run/committed/date/@unixtime[. > 0] return
attribute commit {$v - exactly-one($run/validated/date/@unixtime)} }
{ for $v in $run/prepared/date/@unixtime[. > 0 and $totalTasks >
0] return attribute preparationPerTask {($v -
exactly-one($run/started/date/@unixtime)) div $totalTasks} }
{ for $v in $run/executed/date/@unixtime[. > 0 and $totalTasks >
0] return attribute executionPerTask {($v -
exactly-one($run/prepared/date/@unixtime)) div $totalTasks} }
{ for $v in $run/validated/date/@unixtime[. > 0 and $totalTasks
> 0] return attribute validationPerTask {($v -
exactly-one($run/executed/date/@unixtime)) div $totalTasks} }
{ for $v in $run/committed/date/@unixtime[. > 0 and $totalTasks
> 0] return attribute commitPerTask {($v -
exactly-one($run/validated/date/@unixtime)) div $totalTasks} }
{attribute allowedExecution
{exactly-one($run/allowedExecutionTime) * 1000}}
{attribute successCount {$successCount}}
{attribute failureCount {$failureCount}}
{attribute nooutputCount {$nooutputCount}}
{attribute totalTasks {$totalTasks}}
</run>
