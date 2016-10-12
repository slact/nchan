#!/bin/zsh

seconds_per_day=0.04
file_resolution="640x320"
git log --no-walk --tags --reverse --simplify-by-decoration --pretty="format:%ct|%D" > captions.txt

gource_opt=(
    --highlight-dirs
    --multi-sampling
    --camera-mode overview
    -c 4
    --seconds-per-day $seconds_per_day
    --max-file-lag 1
    --auto-skip-seconds 1
    --date-format "%b %d %Y"
    --caption-file ./captions.txt
    --caption-duration 10
    --caption-offset 1
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
    gource_opt+=(--viewport $file_resolution
		 --hide mouse,progress)
    gource ./ $gource_opt -o - | ffmpeg $ffmpeg_opt $1
fi

rm captions.txt
