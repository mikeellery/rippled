#!/usr/bin/env bash

USAGE="Usage: $0 [--loose] file1 file2 file3 ... fileN"

if (( ! $# >= 1 )); then
	echo "$USAGE"
    echo "At least one source name required"
    exit 1;
fi

# Allow for loose matching (regex instead of equality
# when comparing args to avialable target names)
exactmatch=true
if [ $1 = "-l" ] || [ $1 = "--l" ] || [ $1 = "--loose" ] ; then
    shift
    exactmatch=false
fi

if (( ! $# >= 1 )); then
	echo "$USAGE"
    echo "At least one source name required"
    exit 1;
fi

# We expect this to be run only
# from a cmake build directory, and only
# the unix makefile target is supported.
# On windows, it looks like the NMake target
# might be an option, but is currently untested
# AND you'd still need bash on windows.
if [ ! -f Makefile ]; then
    echo "Makefile not found in current dir - run CMake or change directories"
    exit 1;
fi

echo -n "Building..."
make 2>&1 1>/dev/null
echo "..Done"

# Loop over the output of 'make help' to get a list of
# available proprocessed file targets. Compare those
# against the argument(s) passed in.
declare -ar args=("$@");
declare -a targets=()
declare -a available=()
while read -r maketarg; do
    cleantarg=( $(sed -e "s/^... //g" <<< "$maketarg") )
    if [[ $cleantarg =~ \.i$ ]] ; then
        filename=$(basename "$cleantarg")
        extension="${filename##*.}"
        file="${filename%.*}"
        available+=( "$file" )
        for match in "${args[@]}"; do
            if [ "$exactmatch" = true ] ; then
                if [ $file = $match ]; then
                    targets+=( "$cleantarg" )
                fi
            else
                if [[ $cleantarg =~ "$match" ]] ; then
                    targets+=( "$cleantarg" )
                fi
            fi
        done
    fi
done < <(make help)

# Verify that at least one target was found
if (( ${#targets[@]} == 0 )); then
    echo "No matching targets found - check spelling ?"
    echo "Here are the available preprocessing targets:"
    IFS=$'\n' available_sorted=($(sort <<<"${available[*]}"))
    unset IFS
    for avail in "${available_sorted[@]}"; do
        echo "   $avail"
    done
    exit 1;
fi

# Run through the targets and run make to generate the .i file.
# Then extract the # lines from the proprocessed file to extract
# the includes. We further filter on strings matching /src/ to
# narrow the report to ripple sources, but this filtering might
# need to be adjusted.
for target in "${targets[@]}"; do
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    echo "Evaluating $target";
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    filename=$(basename "$target")
    extension="${filename##*.}"
    file="${filename%.*}"
    make $target 2>&1 1>/dev/null
    dotifile=$(find ./CMakeFiles | grep -e "/$file\..*\.i$")
    grep "^#" "$dotifile" |\
        cut -d " " -f 3 |\
        sed -e 's/^ *"//g' -e 's/" *$//g' |\
        sort | uniq |\
        grep "/src/" |\
        sed -E -e "s/^.+\/src\//   src\//g"
done


