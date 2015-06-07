using System;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Security;
using System.Security.Permissions;
using System.Security.Principal;
using Microsoft.Win32.SafeHandles;

namespace RefreshExcelReports
{

    // Code courtesy of: http://stackoverflow.com/questions/125341/how-do-you-do-impersonation-in-net
    // Logon as batch: http://www.brooksnet.com/faq/117-02.html
    [PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
    public class Impersonation : IDisposable
    {
        private readonly SafeTokenHandle _handle;
        private readonly WindowsImpersonationContext _context;

        const int LOGON32_LOGON_NEW_CREDENTIALS = 9;
        const int LOGON32_LOGON_BATCH = 4;
        const int LOGON32_LOGON_INTERACTIVE = 2;

        public Impersonation(string domain, string username, string password)
        {
            var ok = LogonUser(username, domain, password,
                            LOGON32_LOGON_BATCH, 0, out this._handle);
            if (!ok)
            {
                var errorCode = Marshal.GetLastWin32Error();
                throw new ApplicationException(string.Format("Could not impersonate the elevated user.  LogonUser returned error code {0}.", errorCode));
            }

            WindowsIdentity ident = new WindowsIdentity(this._handle.DangerousGetHandle());
            this._context = ident.Impersonate();
        }

        public void Dispose()
        {
            _context.Undo();
            this._context.Dispose();
            this._handle.Dispose();
        }

        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern bool LogonUser(String lpszUsername, String lpszDomain, String lpszPassword, int dwLogonType, int dwLogonProvider, out SafeTokenHandle phToken);

        public sealed class SafeTokenHandle : SafeHandleZeroOrMinusOneIsInvalid
        {
            private SafeTokenHandle()
                : base(true) { }

            [DllImport("kernel32.dll")]
            [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
            [SuppressUnmanagedCodeSecurity]
            [return: MarshalAs(UnmanagedType.Bool)]
            private static extern bool CloseHandle(IntPtr handle);

            protected override bool ReleaseHandle()
            {
                return CloseHandle(handle);
            }
        }
    }
}
