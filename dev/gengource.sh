#!/bin/zsh


gource_opt=(
    --highlight-dirs
    --multi-sampling
    --camera-mode overview
    -c 4
    --seconds-per-day 0.07
    --max-file-lag 1
    --auto-skip-seconds 1
    --date-format "%b %d %Y"
    --viewport 640x320
)

ffmpeg_opt=(
    -y
    -r 60
    -f image2pipe
    -vcodec ppm
    -i -
    -vcodec libx264
    -pix_fmt yuv420p
    -threads 0
    -bf 0
    -preset veryslow
    -crf 28
)



if [[ -z $1 ]]; then
    gource ./ $gource_opt
else
    gource ./ $gource_opt -o - | ffmpeg $ffmpeg_opt $1
fi
