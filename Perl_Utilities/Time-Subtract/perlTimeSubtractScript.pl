# A simple script to subtract two times that are within the same 12 hour block,
# which should be sufficient for my purposes of comparing performance of reports
# run in dev vs prod.
# Author: Barret Miller

print "Enter the start time in hh:mm:ss format.\n";
$time = <STDIN>;
@startTime = split(/:/, $time);
$startHours = $startTime[0];
$startMinutes = $startTime[1];
$startSeconds = $startSeconds[2];

print "Enter the stop time in hh:mm:ss format.\n";
$time = <STDIN>;
@stopTime = split(/:/, $time);
$stopHours = $stopTime[0];
$stopMinutes = $stopTime[1];
$stopSeconds = $stopSeconds[2];

$totalHours = $stopHours - $startHours;
$totalMinutes = $stopMinutes - $startMinutes;
$totalSeconds = $stopSeconds - $startSeconds;

if($totalSeconds < 0)
{
  $totalSeconds = $totalSeconds + 60;
  $totalMinutes--;
}

if($totalMinutes < 0)
{
  $totalMinutes = $totalMinutes + 60;
  $totalHours--;
}

print "The total time elapsed is: $totalHours h:$totalMinutes m:$totalSeconds s\n";
