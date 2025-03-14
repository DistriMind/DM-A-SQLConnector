# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| >=1.0.0 | :heavy_check_mark: |
| <1.0.0  | :x:                |

## Reporting a Vulnerability

To report a vulnerability, please report security issues related to the project to the
following email address:

    jason.mahdjoub@distri-mind.fr. 

## Verifying contents

All commits are signed. All artifacts published on [artifactory.distri-mind.fr](https://artifactory.distri-mind.fr) and on Maven central are signed. 
For each artifact, there is an associated signature file with the .asc suffix.

To verify the signature use [this public key](key-2023-10-09.pub). Here is its fingerprint:
```
pub   rsa4096/2CDFE2681CA78FB0 2023-10-09 [SC]
    fingerprint = DF24 38C0 BA4E 54BE 44A7  08F8 2CDF E268 1CA7 8FB0
uid   Jason Mahdjoub <jason.mahdjoub@distri-mind.fr>
sub   rsa4096/418D774BEDA1D018 2023-10-09 [E]
```

A copy of this key is stored on the
[keyserver.ubuntu.com](https://keyserver.ubuntu.com/) keyserver. To add it to
your public key ring use the following command:

```
> gpg  --keyserver hkps://keyserver.ubuntu.com --recv-keys DF2438C0BA4E54BE44A708F82CDFE2681CA78FB0
```

## Preventing commit history overwrite

In order to prevent loss of commit history, developers of the project
are highly encouraged to deny branch deletions or history overwrites
by invoking the following two commands on their local copy of the
repository.


```
git config receive.denyDelete true
git config receive.denyNonFastForwards true
```