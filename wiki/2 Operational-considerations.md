# Data longevity

Nippy is widely used to store **long-lived** data and promises (as always) that **data serialized today should be readable by all future versions of Nippy**.

But please note that the **converse is not generally true**:

- Nippy `vX` **should** be able to read all data from Nippy `vY<=X` (backwards compatibility)
- Nippy `vX` **may/not** be able to read all data from Nippy `vY>X` (forwards compatibility)

# Rolling updates and rollback

From time to time, Nippy may introduce:

- Support for serializing **new types**
- Optimizations to the serialization of **pre-existing types**

To help better support **rolling deployments** and **rollback**, Nippy will always introduce such changes over **two version releases**:

- Release 1: to add **read support** for the new types
- Release 2: to add **write support** for the new types

So if you use **rolling deployments** please ensure you update **step by step** (e.g. v3.5 → v3.6 → v3.7) so that read support is fully deployed before the corresponding writes might happen.

# Stability of byte output

It has never been an objective of Nippy to offer **predictable byte output**, and I'd generally **recommend against** depending on specific byte output.

However, I know that a small minority of users *do* have specialized needs in this area.

So starting with Nippy v3.4, Nippy's release notes will **always clearly indicate** if any changes to byte output are expected.