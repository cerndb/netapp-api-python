%global sum A Python implementation of the NetApp ZAPI
%global srcname netapp-api

Summary: NetApp OCUM API wrapper
Name: python-netapp-api
Version: 0.9.0
Release: 1%{?dist}
Source0: %{name}-%{version}.tar.gz
License: GPLv3
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
BuildRequires: python2-devel python34-devel python-setuptools python34-setuptools
BuildRequires: python2-devel python-setuptools
Requires: python-requests pytz python-lxml
Vendor: Albin Stjerna <albin.stjerna@cern.ch>
Url: https://github.com/cerndb/netapp-api-python

%description
This is a Python implementation of relevant parts of NetApps ZAPI. It
is meant to provide a higher-level API than the generated bindings
provided by NetApp and packaged as cerndb-sw-python-NetApp, but works
completely independently of them.

%package -n python2-%{srcname}
Summary:        %{sum}
Requires: python-requests pytz python-lxml
%description -n python2-%{srcname}
%{description}

%package -n python3-%{srcname}
Summary:        %{sum}
Requires: python34-requests python34-pytz python3-lxml
%description -n python3-%{srcname}
%{description}

%prep
%setup -q

%build
%{__python3} setup.py build  --executable=%{__python2}
%{__python2} setup.py build  --executable=%{__python2}

%install
%{__rm} -rf %{buildroot}

%{__python3} setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --prefix=/usr --record=INSTALLED_FILES_3
%{__python2} setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --prefix=/usr --record=INSTALLED_FILES_2

%clean
rm -rf $RPM_BUILD_ROOT

%files -n python2-%{srcname}
%doc README.md
%{python2_sitelib}/*
%defattr(-,root,root,-)

%files -n python3-%{srcname}
%doc README.md
%{python3_sitelib}/*
%defattr(-,root,root)
