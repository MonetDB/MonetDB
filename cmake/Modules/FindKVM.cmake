# - Find kvm
# Find the native kvm headers and libraries.
#
# KVM_INCLUDE_DIR	- where to find kvm.h, etc.
# KVM_LIBRARIES	- List of libraries when using kvm.
# KVM_FOUND	- True if kvm found.

# Look for the header file.
find_path(KVM_INCLUDE_DIR NAMES kvm.h PATH_SUFFIXES kvm) # On some systems kvm.h is installed under kvm directory.

# Look for the library.
find_library(KVM_LIBRARIES NAMES kvm)

# Handle the QUIETLY and REQUIRED arguments and set KVM_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(KVM DEFAULT_MSG KVM_LIBRARIES KVM_INCLUDE_DIR)

mark_as_advanced(KVM_INCLUDE_DIR KVM_LIBRARIES)

if(KVM_FOUND)
  add_library(KVM::KVM UNKNOWN IMPORTED)
  set_target_properties(KVM::KVM
    PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${KVM_INCLUDE_DIR}")
  set_target_properties(KVM::KVM
    PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${KVM_LIBRARIES}")
endif()
