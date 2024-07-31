RZO
===
A Business Application Framework

`RZO` is a free and open business application framework that is:

- **Mobile.** Other business applications have mobility added on top of a much older
  legacy infrastructure. RZO has mobility built-in from the start.
- **Offline/Disconnected.** RZO works with offline/disconnected clients (mobile or otherwise)
  and synchronizes once connection has been reestablished.
- **Scalable.** RZO is built to scale: from a small business with a sole proprietor to multinational
  enterprises with millions of users.
- **Hybrid Infrastructure.** RZO runs in its entirety on a small laptop with a local DB and single server
  process and scales all the way up to containerized cloud infrastructure such as Kubernetes connected to
  database clusters.


Infrastructure
--------------

RZO is a pure JavaScript based implementation. This allows the **same** code to be run on small devices
and Web browsers as well as servers, virtual machines and container-based enterprise infrastructure.
The data storage is provided by PostgreSQL on the server side(s) and CouchDB-compatible databases on the
client side. The data backend is fully compatible with the Couch Replication Protocol and can
therefore synchronize with small devices in an offline/online mode as well as other PostgreSQL databases in
a fully distributed / clustered database infrastructure.


Data Integrity and Traceability
-------------------------------

Every change made to a record in RZO is fully audited and version-controlled. This allows for offline/online
replication to work properly and to resolve data conflicts that will inevitably occur with changes made while
devices are offline. This also makes it possible for RZO to be considered for highly sensitive applications
and industries such as Energy/Utilities, Pharmaceutical, Nuclear, Defense and Government.


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
- Do not make comments, remarks or suggestions that utilize or imply concepts of "sensitivity", "inclusivity",
  "social", "society", "diversity", "equity", "equality", "concience", "rightness", "wrongful", "historical",
  "sexuality", "identity", "gender", "ancestry" or any derivative of those concepts.
- Do not make comments, remarks or suggestions that try to circumvent the terms used above but still convey
  the same sentiments.
- Any and all suggested changes can be rejected or ignored without specifying any reason for doing so.
- There is no challenge, escalation or appeal process for rejections.
- If your suggested change was rejected without any reason provided, your change likely broke one of the
  rules stated in this list.

