//! RAM for the 4.5 GiB FishHashPlus dataset, using large pages when the OS allows it.
//!
//! FishHashPlus reads 96 random 128-byte items per hash. With 4 KiB pages almost every read
//! also misses the TLB; 2 MiB pages cover the whole dataset with ~2,300 TLB entries instead of
//! ~1.2 million, which removes most of that extra latency.
//!
//! * Windows: `VirtualAlloc(MEM_LARGE_PAGES)`. Needs the "Lock pages in memory" right
//!   (SeLockMemoryPrivilege), the same one XMRig asks for. Without it, normal pages are used.
//! * Linux: transparent huge pages via `madvise(MADV_HUGEPAGE)`.

use anyhow::{Context, Result};
use log::{info, warn};
use memmap2::{MmapMut, MmapOptions};

pub struct DatasetMemory {
    inner: Inner,
    len: usize,
}

enum Inner {
    Mapped(MmapMut),
    #[cfg(windows)]
    LargePages(*mut u8),
}

// The large-page pointer is owned exclusively by this struct and only read after construction.
unsafe impl Send for DatasetMemory {}
unsafe impl Sync for DatasetMemory {}

impl DatasetMemory {
    /// Allocates `len` bytes. `use_large_pages = false` forces normal pages.
    pub fn allocate(len: usize, use_large_pages: bool) -> Result<Self> {
        #[cfg(windows)]
        if use_large_pages {
            match windows_large::allocate(len) {
                Ok(ptr) => {
                    info!("Dataset uses 2 MiB large pages");
                    return Ok(Self {
                        inner: Inner::LargePages(ptr),
                        len,
                    });
                }
                Err(reason) => {
                    warn!(
                        "Large pages unavailable ({reason}); using normal 4 KiB pages. {}",
                        windows_large::HOW_TO_ENABLE
                    );
                }
            }
        }

        let map = MmapOptions::new()
            .len(len)
            .map_anon()
            .context("allocating dataset memory")?;
        #[cfg(target_os = "linux")]
        if use_large_pages {
            match map.advise(memmap2::Advice::HugePage) {
                Ok(()) => info!("Dataset uses transparent huge pages"),
                Err(err) => warn!(
                    "Transparent huge pages unavailable for the dataset ({err}); using 4 KiB pages"
                ),
            }
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        let _ = use_large_pages;
        Ok(Self {
            inner: Inner::Mapped(map),
            len,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            Inner::Mapped(map) => &map[..self.len],
            #[cfg(windows)]
            Inner::LargePages(ptr) => unsafe { std::slice::from_raw_parts(*ptr, self.len) },
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.inner {
            Inner::Mapped(map) => &mut map[..self.len],
            #[cfg(windows)]
            Inner::LargePages(ptr) => unsafe { std::slice::from_raw_parts_mut(*ptr, self.len) },
        }
    }
}

impl Drop for DatasetMemory {
    fn drop(&mut self) {
        #[cfg(windows)]
        if let Inner::LargePages(ptr) = self.inner {
            windows_large::free(ptr);
        }
    }
}

#[cfg(windows)]
mod windows_large {
    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, ERROR_NOT_ALL_ASSIGNED, HANDLE, LUID,
    };
    use windows_sys::Win32::Security::{
        AdjustTokenPrivileges, LookupPrivilegeValueW, LUID_AND_ATTRIBUTES, SE_PRIVILEGE_ENABLED,
        TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES, TOKEN_QUERY,
    };
    use windows_sys::Win32::System::Memory::{
        GetLargePageMinimum, VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE,
        MEM_RESERVE, PAGE_READWRITE,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    pub const HOW_TO_ENABLE: &str =
        "To enable: run secpol.msc → Local Policies → User Rights Assignment → \
        \"Lock pages in memory\" → add your user, then sign out and back in (or reboot).";

    fn enable_lock_memory_privilege() -> Result<(), String> {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(
                GetCurrentProcess(),
                TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
                &mut token,
            ) == 0
            {
                return Err(format!("OpenProcessToken failed ({})", GetLastError()));
            }
            let name: Vec<u16> = "SeLockMemoryPrivilege\0".encode_utf16().collect();
            let mut luid = LUID {
                LowPart: 0,
                HighPart: 0,
            };
            if LookupPrivilegeValueW(std::ptr::null(), name.as_ptr(), &mut luid) == 0 {
                CloseHandle(token);
                return Err(format!("LookupPrivilegeValue failed ({})", GetLastError()));
            }
            let privileges = TOKEN_PRIVILEGES {
                PrivilegeCount: 1,
                Privileges: [LUID_AND_ATTRIBUTES {
                    Luid: luid,
                    Attributes: SE_PRIVILEGE_ENABLED,
                }],
            };
            let ok = AdjustTokenPrivileges(
                token,
                0,
                &privileges,
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            let err = GetLastError();
            CloseHandle(token);
            if ok == 0 {
                return Err(format!("AdjustTokenPrivileges failed ({err})"));
            }
            if err == ERROR_NOT_ALL_ASSIGNED {
                return Err("this user does not have the \"Lock pages in memory\" right".into());
            }
            Ok(())
        }
    }

    pub fn allocate(len: usize) -> Result<*mut u8, String> {
        enable_lock_memory_privilege()?;
        unsafe {
            let page = GetLargePageMinimum();
            if page == 0 {
                return Err("the CPU or OS does not support large pages".into());
            }
            let size = len.div_ceil(page) * page;
            let ptr = VirtualAlloc(
                std::ptr::null(),
                size,
                MEM_RESERVE | MEM_COMMIT | MEM_LARGE_PAGES,
                PAGE_READWRITE,
            );
            if ptr.is_null() {
                // 1450 = not enough contiguous physical memory (fragmentation); a reboot usually helps.
                return Err(format!(
                    "VirtualAlloc(MEM_LARGE_PAGES) failed ({}); free RAM may be too fragmented",
                    GetLastError()
                ));
            }
            Ok(ptr as *mut u8)
        }
    }

    pub fn free(ptr: *mut u8) {
        unsafe {
            VirtualFree(ptr as _, 0, MEM_RELEASE);
        }
    }
}
