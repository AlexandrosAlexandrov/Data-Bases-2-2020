# Set the intermediate directory.
IntDir = "bin-int"

# Build the executable.
build: HP HT Demo
	@gcc $(IntDir)/HP.obj $(IntDir)/HT.obj $(IntDir)/Demo.obj BF/BF_64.a -lm -no-pie -o demo

# Compile the translation units.
HP: HP.c | SetupDir
	@gcc HP.c -c -o $(IntDir)/$@.obj

HT: HT.c | SetupDir
	@gcc HT.c -c -o $(IntDir)/$@.obj

Demo: Demo.c | SetupDir
	@gcc Demo.c -c -o $(IntDir)/$@.obj

# Cleans the directory.
clean:
	@rm -f -r $(IntDir)
	@rm -f demo

# Setup the project directory.
SetupDir:
	@mkdir -p $(IntDir)
