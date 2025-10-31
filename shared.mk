# This is where shared snippets of extension-specific Makefiles go

# PostgreSQL does not allow declaration after statement, but we do
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS)) -Werror=implicit-function-declaration
