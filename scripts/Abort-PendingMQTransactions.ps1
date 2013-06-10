Param(
  [string]$queueManager = "AcceptanceTests"
)

dspmqtrn | foreach-object{ 
    $prefix = "AMQ7056: Transaction number "

    $index = $_.IndexOf($prefix)

    if($index -lt 0){
        return
    }

    $start = $index + $prefix.length

    $end = $_.IndexOf(" ",$start)


    $txNumber = $_.Substring($start,$end-$start)

    
    $arguments = 'rsvmqtrn -m ' + $queueManager + ' -b "' + $txNumber + '"'

    Invoke-Expression $arguments
}
