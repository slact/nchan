#!/bin/bash
while true; do
    php ./fdleak.php
    ss -xp | grep ncnan | wc -l
done
