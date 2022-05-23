.. _swh-dataset-schema:

Relational schema
=================

The Merkle DAG of the Software Heritage archive is encoded in the dataset as a
set of relational tables.

This page documents the relational schema of the **latest version** of the
graph dataset.

..
    A simplified view of the corresponding database schema is shown here:

    .. image:: _images/dataset-schema.svg

**Note**: To limit abuse, some columns containing personal information are
pseudonimized in the dataset using a hash algorithm. Individual authors may be
retrieved by querying the Software Heritage API.

- **content**: contains information on the contents stored in
  the archive.

  - ``sha1`` (string): the SHA-1 of the content (hexadecimal)
  - ``sha1_git`` (string): the Git SHA-1 of the content (hexadecimal)
  - ``sha256`` (string): the SHA-256 of the content (hexadecimal)
  - ``blake2s256`` (bytes): the BLAKE2s-256 of the content (hexadecimal)
  - ``length`` (integer): the length of the content
  - ``status`` (string): the visibility status of the content

- **skipped_content**: contains information on the contents that were not
  archived for various reasons.

  - ``sha1`` (string): the SHA-1 of the skipped content (hexadecimal)
  - ``sha1_git`` (string): the Git SHA-1 of the skipped content (hexadecimal)
  - ``sha256`` (string): the SHA-256 of the skipped content (hexadecimal)
  - ``blake2s256`` (bytes): the BLAKE2s-256 of the skipped content
    (hexadecimal)
  - ``length`` (integer): the length of the skipped content
  - ``status`` (string): the visibility status of the skipped content
  - ``reason`` (string): the reason why the content was skipped

- **directory**: contains the directories stored in the archive.

  - ``id`` (string): the intrinsic hash of the directory (hexadecimal),
    recursively computed with the Git SHA-1 algorithm

- **directory_entry**: contains the entries in directories.

  - ``directory_id`` (string): the Git SHA-1 of the directory
    containing the entry (hexadecimal).
  - ``name`` (bytes): the name of the file (basename of its path)
  - ``type`` (string): the type of object the branch points to (either
    ``revision``, ``directory`` or ``content``).
  - ``target`` (string): the Git SHA-1 of the object this
    entry points to (hexadecimal).
  - ``perms`` (integer): the permissions of the object


- **revision**: contains the revisions stored in the archive.

  - ``id`` (string): the intrinsic hash of the revision (hexadecimal),
    recursively computed with the Git SHA-1 algorithm. For Git repositories,
    this corresponds to the commit hash.
  - ``message`` (bytes): the revision message
  - ``author`` (string): an anonymized hash of the author of the revision.
  - ``date`` (timestamp): the date the revision was authored
  - ``date_offset`` (integer): the offset of the timezone of ``date``
  - ``committer`` (string): an anonymized hash of the committer of the revision.
  - ``committer_date`` (timestamp): the date the revision was committed
  - ``committer_date_offset`` (integer): the offset of the timezone of
    ``committer_date``
  - ``directory`` (string): the Git SHA-1 of the directory the revision points
    to (hexadecimal). Every revision points to the root directory of the
    project source tree to which it corresponds.

- **revision_history**: contains the ordered set of parents of each revision.
  Each revision has an ordered set of parents (0 for the initial commit of a
  repository, 1 for a regular commit, 2 for a regular merge commit and 3 or
  more for octopus-style merge commits).

  - ``id`` (string): the Git SHA-1 identifier of the revision (hexadecimal)
  - ``parent_id`` (string): the Git SHA-1 identifier of the parent (hexadecimal)
  - ``parent_rank`` (integer): the rank of the parent, which defines the
    ordering between the parents of the revision

- **release**: contains the releases stored in the archive.

  - ``id`` (string): the intrinsic hash of the release (hexadecimal),
    recursively computed with the Git SHA-1 algorithm
  - ``target`` (string): the Git SHA-1 of the object the release points to
    (hexadecimal)
  - ``date`` (timestamp): the date the release was created
  - ``author`` (integer): the author of the revision
  - ``name`` (bytes): the release name
  - ``message`` (bytes): the release message

- **snapshot**: contains the list of snapshots stored in the archive.

  - ``id`` (string): the intrinsic hash of the snapshot (hexadecimal),
    recursively computed with the Git SHA-1 algorithm.

- **snapshot_branch**: contains the list of branches associated with
  each snapshot.

  - ``snapshot_id`` (string): the intrinsic hash of the snapshot (hexadecimal)
  - ``name`` (bytes): the name of the branch
  - ``target`` (string): the intrinsic hash of the object the branch points to
    (hexadecimal)
  - ``target_type`` (string): the type of object the branch points to (either
    ``release``, ``revision``, ``directory`` or ``content``).

- **origin**: the software origins from which the projects in the dataset were
  archived.

  - ``url`` (bytes): the URL of the origin

- **origin_visit**: the different visits of each origin. Since Software
  Heritage archives software continuously, software origins are crawled more
  than once. Each of these "visits" is an entry in this table.

  - ``origin``: (string) the URL of the origin visited
  - ``visit``: (integer) an integer identifier of the visit
  - ``date``: (timestamp) the date at which the origin was visited
  - ``type`` (string): the type of origin visited (e.g ``git``, ``pypi``, ``hg``,
    ``svn``, ``git``, ``ftp``, ``deb``, ...)

- **origin_visit_status**: the status of each visit.

  - ``origin``: (string) the URL of the origin visited
  - ``visit``: (integer) an integer identifier of the visit
  - ``date``: (timestamp) the date at which the origin was visited
  - ``type`` (string): the type of origin visited (e.g ``git``, ``pypi``, ``hg``,
    ``svn``, ``git``, ``ftp``, ``deb``, ...)
  - ``snapshot_id`` (string): the intrinsic hash of the snapshot archived in
    this visit (hexadecimal).
  - ``status`` (string): the integer identifier of the snapshot archived in
    this visit, either ``partial`` for partial visits or ``full`` for full
    visits.
