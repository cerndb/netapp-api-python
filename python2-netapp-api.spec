%define release 1

Summary: NetApp OCUM API wrapper
Name: python2-netapp-api
Version: 0.4.2
Release: 2
Source0: %{name}-%{version}.tar.gz
License: GPLv3
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
BuildRequires: python2-devel python-setuptools
Requires: python-requests pytz python-lxml
Vendor: Albin Stjerna <albin.stjerna@cern.ch>
Url: https://github.com/cerndb/netapp-api-python

%description
This is a Python implementation of relevant parts of NetApp's ZAPI.

%prep
%setup -n %{name}-%{version} -n %{name}-%{version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
