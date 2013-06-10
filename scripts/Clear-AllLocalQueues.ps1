"DISPLAY QLOCAL(*)" | runmqsc | foreach-object{ 

    $index = $_.IndexOf("QUEUE")

    if($index -lt 1){
        return
    }

    $start = $index+6

    $end = $_.IndexOf(")",$start)


    $queueName = $_.Substring($start,$end-$start)

    if($queueName.StartsWith("SYSTEM.")){
        return
    }

    if($queueName.StartsWith("AMQ.")){
        return
    }

    "Deleting $queueName"

    "DELETE QLOCAL('$queueName') PURGE" | runmqsc
}
