RZO
===
A Business Application Framework

`RZO` is a free and open business application framework that is:

- **Mobile.** Other business applications have mobility added on top of a much older
  legacy infrastructure. RZO has mobility as a core requirement from the beginning.
- **Offline/Disconnected.** RZO aims to work with offline/disconnected clients (mobile or otherwise)
  that synchronize once connection has been reestablished.
- **Scalable.** RZO is designed to scale from a small business with a sole proprietor to multinational
  enterprises with millions of customers as users.
- **Hybrid Infrastructure.** RZO runs in its entirety on a small laptop with a local DB and single server
  process and scales all the way up to containerized cloud infrastructure such as Kubernetes connected to
  database clusters.


Infrastructure
--------------

RZO is a pure JavaScript based implementation. This allows the **same** code to be run on small devices
and Web browsers as well as servers, virtual machines and container-based enterprise infrastructure.
The data storage is provided by PostgreSQL on the server side(s) and CouchDB-compatible databases on the
client side. The data backend aims to be fully compatible with the Couch Replication Protocol in order to
synchronize with small devices in an offline/online mode as well as other PostgreSQL databases in
a fully distributed / clustered database infrastructure.


Data Integrity and Traceability
-------------------------------

Every change made to a record in RZO must be fully audited and version-controlled. This allows for offline/online
replication to work properly and to resolve data conflicts that will inevitably occur with changes made while
devices are offline. This also makes it possible for RZO to be considered for highly sensitive applications
and industries such as Energy/Utilities, Pharmaceutical, Nuclear, Defense and Government.


Key Concepts
------------

Being a business application **framework**, RZO doesn't define or lock in specific business application
objects or logic. The specific objects in the repository serve to illustrate common traits that are found
in business applications such as Product Management, Procurement, Finance, Work Management, Human Resources,
Inventory Management, Workforce Planning/Scheduling, etc. You are free to define, extend or modify your
own objects. In fact, you are encouraged to publish and share your business application in the hopes that
a full ecosystem of business objects, applications and functionality is created from which end-users can
pick and choose.

A typical RZO application utilizes the following concepts found in the framework:

- Entity
- Field
- State
- Collection
- Row and ResultSet
- Persona
- Policy
- Source


Entity
------

An `Entity` describes something that is stored. It could be a 'thing' such as a user or customer, or it could
be an 'action' such as a purchase or a work order. You can think of an Entity as a table in a database, and
while that is true, it's not the full story, as you'll learn later on.


Field
-----

Each `Entity` consists of `Fields`. Fields describe attributes on the Entity, for example, the name,
address and city for a person. While not quite the same, you can think of Fields as columns in a table.


State
-----

RZO explicitly separates data and metadata: `Entities` and `Fields` describe the metadata for a business
object: how many fields, their names and **types** of data it users such as text, numbers, dates, etc.
`State` holds the actual data for a given Entity. For example, if the Entity for a "customer" describes it as
having a Name, City and Last Order Date, then `State` holds an actual customer "Joe", "Houston" and "June 1".
Perhaps it's easier to describe it this way: when a given server is being used to service 10,000 customers at
a given moment, that server only has one (1) instance of the object "Entity: Customer", but 10,000
instances of the 'State' object. `State` is designed to be as small and simple as possible to conserve resources
and make RZO scale easily.
If you've dealt with extending or programming other business applications, you'll probably have to get used
to the idea that `Entities` and `Fields` do not 'hold' or 'store' any data, they operate on State objects that hold
data instead.


Collection
----------

A `Collection` describes a list of Entities. You can think of a Collection as a database query. While that is how
a Collection will be constructed when asked, it's more powerful than that. For example, Collections can dynamically
adapt based on *who* is asking for the data. A list of pending work orders would change based on who those work orders
are for: even though they both query the same collection, Charley sees WO101 and WO102, but Alice sees WO203 because
they work in different geographic areas, or because Charley is a mechanic and Alice is a powerline inspector.


Row and ResultSet
-----------------

A `Row` is how data is passed around in RZO, both within the client or server, as well as in between the client
and server. It is designed to be as lean as possible. In fact, it's a very thin wrapper around the standard JavaScript
Object with the actual data being held in that JavaScript object. `Row` wraps several convenience and standardization
methods around the standard JavaScript Object that handle things like 'undefined' vs. 'null', conversions to/from
strings, etc.
A `ResultSet` is an iterable list of `Rows`. In its simplest form, it's a JavaScript Array of `Rows`, but not all lists
are equal: for example, small mobile devices are better served with 'paging' lists, to conserve memory and reduce
'stickiness' when users ask for a lot of data. ResultSet manages the complications of all that.


Persona
-------

A `Persona` encapsulates the job position and authorization of an individual that's logged into RZO at any given
time. A machine shop mechanic needs different access and data than a VP of Finance to do their daily tasks. RZO
separates the user login process from their Persona: authentication is best handled by Identity Providers using
OpenID/SAML outside of RZO, either in-house or hosted. Once authenticated, the user's authorization profile - what
they are allowed to see and do in RZO - is managed via Personas. A user can have (or "be") multiple Personas, but
only one Persona is active at a time.


Policy
------

A `Policy` controls what data users have access to, and what they are allowed to do with that data in terms of
modifications. Just like `Collections`, `Policies` are dynamic. For example, a Policy that controls access to
work orders can allow Charley to view and edit WO101, but only view WO203, because the Policy that's
assigned to his 'Mechanic' `Persona` prohibits editing the 'Inspection' work order WO203.


Source
------

RZO allows data to come from various places *at the same time*. For example, a mobile client tool should be
able to pull data from a live server that can't be cached locally for storage / performance reasons as well
as work with data from its local storage when in offline mode. A `Source` abstracts the actual interaction
with a server or backend so that `Collections` don't need to know the underlying protocol to obtain their
data.


Why GPLv3?
----------

The choice to go with a strong copy-left license wasn't made lightly. It is fully understood that a business
application framework will not work without an ecosystem of software, consulting and implementation services
companies that need to get their work paid for. GPLv3 can be a controversial subject for those outfits and
sometimes their customers because of the copy-left aspect. In layman's terms, here's what's intended to be
possible and not possible under that license. **PLEASE NOTE** that the following list does not supercede, alter or
change the license as specified in LICENSE. **Always follow LICENSE**.

- Extensions or modifications that, for example, implement business logic for you (if you are the end-user)
  or your customer (if you are a service provider) do not have to be made public (copy-left) AS LONG AS
  they remain both private to and owned by the end-user.
- If you are a service-provider and wish to re-use/resell/license (or any other means of transferring)
  code extensions or modifications made for customer A, or made in-house without a customer, to customer B,
  you MUST open-source (i.e. copy-left) those changes in accordance with LICENSE.
- A common value proposition of consulting companies to their customers is to state they have 'prebuilt', or
  'best practice', or 'starting kits' for IT implementations. With RZO, those would be open-source. The
  value proposition would therefore shift from having a proprietary 'mouse trap' to their expertise in how
  to implement, support and shepherd those changes to the end-user.


Contributing
------------

RZO is a **non-democratic** and completely **apolitical** project. You are free
to suggest changes to the code or the project itself, but the following rules apply.

- Do not make any comments, remarks and/or suggestions that are social, political, religious or non-technical.
- Do not describe yourself with any form of identity other than your name and (optionally) your
  physical location: city, state/province, country.
- Do not display your race, age, gender, religion, pronouns, sexual orientation or any other identifying trait
  other than described above.
- Do not make non-technical comments, remarks or suggestions that utilize or imply concepts that include
  (but are not limited to) "sensitivity", "inclusivity", "environmental", "social", "society", "diversity",
  "equity", "equality", "concience", "rightness", "wrongful", "historical", "sexuality", "identity",
  "gender", "ancestry" or any derivative of those concepts.
- Do not make comments, remarks or suggestions that try to circumvent the terms used above but still convey
  the same sentiments.
- Any and all suggested changes can be rejected or ignored without specifying any reason for doing so.
- There is no challenge, escalation or appeal process for rejections.
- If your suggested change was rejected without any reason provided, your change likely broke one of the
  rules stated in this list.

