#!/bin/bash
while true; do
    php ./fdleak.php
    ss -xp | grep nchan | wc -l
done
