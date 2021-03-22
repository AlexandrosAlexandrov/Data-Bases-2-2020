# Set the intermediate directory.
IntDir = "bin-int"

# Build the executable.
build: Common HT SHT Demo
	@gcc $(IntDir)/Common.obj $(IntDir)/HT.obj $(IntDir)/SHT.obj $(IntDir)/Demo.obj BF/BF_64.a -no-pie -o demo

# Compile the translation units.
Common: Common.c | SetupDir
	@gcc Common.c -c -o $(IntDir)/$@.obj

HT: HT.c | SetupDir
	@gcc HT.c -c -o $(IntDir)/$@.obj

SHT: SHT.c | SetupDir
	@gcc SHT.c -c -o $(IntDir)/$@.obj

Demo: Demo.c | SetupDir
	@gcc Demo.c -c -o $(IntDir)/$@.obj

# Cleans the directory.
clean:
	@rm -f -r $(IntDir)
	@rm -f demo

# Setup the project directory.
SetupDir:
	@mkdir -p $(IntDir)
