# D3B CDS Manifest Prep Change History

## Release 1.0.1

### Fixes for sequenicng_file columns

fixes some of the columns in sequencing file

### Summary

- Emojis: 🐛 x1
- Categories: Fixes x1

### New features and changes

- [#150](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/150) - 🐛 Fix sequencing file mappings - [2e81148f](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2e81148f717511819aed6f6f37ba75027b0416e9) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 1.0.0

### CDS Release 2 first draft

This is the first draft of the full release for cds v2. This takes the ideas 
behind the first cds release and changes some configuration based upon the 
spec provided by CDS - Specifically that all of the documents are in one large 
excel file.

### Summary

- Emojis: 🍱 x1, 🐛 x4, ? x3, ♻️ x1, ✨ x2, 🎨 x1, 🌱 x1
- Categories: Additions x4, Fixes x4, Ops x1, Other Changes x4

### New features and changes

- [#146](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/146) - 🍱 add updated sequencing file and excel output - [f91cefab](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/f91cefab4f891808225ef4367189b693a0da687b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#145](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/145) - 🐛 don't save index to excel files - [df23893d](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/df23893d87eecdd54e0150fda04e7fe28f277122) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#144](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/144) - 🐛 get file gatherer working - [afddb9f7](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/afddb9f7e62f44ced949f90f2e21fcd227c87a20) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#143](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/143) -  Sequencing file cache location - [15894491](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/15894491937738cee74502e8b9e3e786410ff0f5) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#142](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/142) - 🐛 get study_tables to have appropriate column values - [b4584307](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b4584307587b85a569201fdfd45fa49634b8f297) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#141](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/141) -  add sequencing_file and updates to study and study_admin, start work on excel_writer - [41fadbd0](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/41fadbd0cb0e623a5e8041a816d8b1a13a400b92) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#140](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/140) - ♻️ move cli options into a common function - [2971b5b2](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2971b5b20b43d9a818d5284c3b83d73d9f8183e2) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#139](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/139) -  Sequencing file update - [6df1b620](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/6df1b6202bf2bd19b27a4e692700c3facbd1580d) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#138](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/138) - ✨ M1TEAM-64 V1.x prep - [f154c2c3](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/f154c2c3fee24c739e29ff3e45915d1d66004b2d) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#137](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/137) - 🎨 remove breakpoint when building jhu manifest - [b0dd7ea1](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b0dd7ea1abe0e090d5fb585bac8ec917e1ace66a) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#136](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/136) - 🐛 load the jhu sample manifest when creating the file-sample-participant map - [b65c934e](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b65c934e4972539438d01ed1f4dcf9b2911c327b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#135](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/135) - ✨ M1TEAM-74 remove jhu samples from the x01 fsp - [7aeb8f3c](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/7aeb8f3c405150923dd9b71761104c1f2c43426b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#133](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/133) - 🌱 Add file-sample-participant mapping for the CBTN X01 - [d13c2105](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/d13c210560c892cccf67211dfae58a1e0faa6327) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.14.1

### updated sample information from redcap

One sample was updated in redcap and another sample had some database updates.
This release has those sample updates.

### Summary

- Emojis: 🍱 x1, 🔥 x1
- Categories: Additions x1, Removals x1

### New features and changes

- [#128](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/128) - 🍱 updated sample manifest after redcap updates - [31e48521](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/31e485215beb87b13c3ee580c2de004b94455e1c) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#127](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/127) - 🔥 Remove unused data files - [71426ec3](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/71426ec3e93d60ab1841f6742d9a3aad4fecfc50) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.14.0

### Updates to diagnoses

This release includes a few updates to diagnoses:

1. remove diagnoses from the histologies file that were manually assigned, that are attached to normal samples.
2. Only include normal samples in the diagnosis-sample mapping table.
3. test that only and all tumor samples are in the diagnosis-sample mapping table.
4. if a participant's only samples in CDS are normal, and all of those samples only come from events that have only normal samples, extract the diagnoses from other events for the participant. Set the diagnosis ID to be based of the C-ID instead of the 7316 ID.

### Summary

- Emojis: 🐛 x2, ✅ x1
- Categories: Additions x1, Fixes x2

### New features and changes

- [#124](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/124) - 🐛 Fix histologies file - [01269fdf](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/01269fdffb570810e6df5c310713a70b7e6e01b9) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#123](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/123) - ✅ add test for diagnosis_sample map file - [38d8ddaa](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/38d8ddaa1584c951f314e78d34fe4c2dbf46e409) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#122](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/122) - 🐛 missing diagnoses and remove normal samples from bs_dx - [ce3e2a9d](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ce3e2a9deb6d8a517be556a0ea252ecf7d58f002) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.13.0

### Set diagnosis as an attribute of event, not aliquot

Set diagnosis as an attribute of event, not aliquot. Set diagnosis IDs as the 7316 (event) ID.

### Summary

- Emojis: ✨ x2, ♻️ x1
- Categories: Additions x3

### New features and changes

- [#118](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/118) - ✨ New diagnosis IDs - [9a5216fd](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/9a5216fdc21d037aefb1db1339c968dd5179a5de) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#117](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/117) - ✨ add qc of diagnoses - [0392c252](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/0392c2520fbf4a4d3ef1d6288d9c504fb7314eb7) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#116](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/116) - ♻️ move qc functions to submodules of qc - [8dd03057](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/8dd03057129355cdea3f2fc7ef0eef0674c47b16) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.12.0

### Use updated base and read counts

Some values were mungled by excel. this unmungles those values.

### Summary

- Emojis: 🍱 x2
- Categories: Additions x2

### New features and changes

- [#113](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/113) - 🍱 updated genomic_info table with correct bases and read counts - [fe87d481](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/fe87d4818d4eeb4c586cf245f2503af16f43a830) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#112](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/112) - 🍱 updated nci submit from bix - [a3a05dbc](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/a3a05dbc38fffd220b7b43b18aa33f1940e77cad) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.11.1

### Remove last `not reported` diagnoses

Updates the diagnosis table with the last diagnosis that were not reported.

### Summary

- Emojis: 🍱 x2, 🚑️ x1
- Categories: Additions x2, Other Changes x1

### New features and changes

- [#110](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/110) - 🍱 remove not reported diagnoses from diagnosis manifest - [c9d85ba8](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/c9d85ba83529029afba6d58cc6cd656bae1f1f38) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#109](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/109) - 🚑️ remove duplicated entries from histologies - [951ae3cb](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/951ae3cbecd1aeba47d97a124455054bc84593f3) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#108](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/108) - 🍱 update histologies file with manual diagnoses - [7ab9493c](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/7ab9493c7bb014ca2a61cdbd3171c832cb19e8e1) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.11.0

### Use updated histologies file

updates the histologies file used for the diagnosis manifest. This is related to #100. This isn't a complete fix,but fixes all the oligo samples.

### Summary

- Emojis: 🍱 x1, ✨ x2, 🔥 x1
- Categories: Additions x3, Removals x1

### New features and changes

- [#106](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/106) - 🍱 updated histologies data - [15f41c79](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/15f41c7974cd7a838f1fdcddb2d1aee23e16a52c) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#105](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/105) - ✨ Generate histologies file - [e8b59bfa](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/e8b59bfa9e54cf80ee679aff5baaa0ddd5b62349) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#104](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/104) - 🔥 remove unused module for utils - [6fd29a6d](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/6fd29a6d3683ac3455b1f6b5a3422946996c3f56) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#103](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/103) - ✨ sort values of all the output manifests - [a49e7140](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/a49e7140a02a9b0a11901a4310e19665a7506aa1) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.10.1

### Add build_manifest docstrings and return manifests

1. add docstrings for 🧑‍💻
2. return output manifests in build_manifest functions. this is so that outputs can be handled in the future. Note that diagnosis output is different from other manifest outputs because it's a tuple of both the diagnosis_table and diagnosis_sample_mapping.

### Summary

- Emojis: 📝 x1, ✨ x1
- Categories: Additions x1, Documentation x1

### New features and changes

- [#101](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/101) - 📝 add docstrings for build_manifest functions - [6b8bb461](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/6b8bb46192d6ea22380cb9b93be9ee1471971eba) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#99](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/99) - ✨ return output tables in build functions - [4962ea90](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/4962ea90c6fba16de4b2c24d2672804999d661b4) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.10.0

### qc sample diagnosis and fix diagnoses 


1. qc the sample-diagnosis mapping file 
2. As discovered in #92, some samples have multiple diagnoses. after investigating this, some diagnoses were found to be mapped to incorrect participants. this fixes that.

### Summary

- Emojis: 🐛 x1, ✨ x1
- Categories: Additions x1, Fixes x1

### New features and changes

- [#93](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/93) - 🐛 reset indices of diagnosis table before splitting and joining - [2100a145](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2100a1450ab6b6fd799d16eaf2bbe5a860b077a8) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#91](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/91) - ✨ add testing for samples in sample diagnoses manifest - [ab70ba0e](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ab70ba0ef88b253ecd418170a82f663d7d19afa2) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.9.0

### Force column Order and fix diagnosis IDs 


1. Column order is now enforced. In previous releases, column order was unstable and could change. Now column order is enforced directly before saving tables to file.
2. Per #84, diagnosis IDs were not unique. diagnosis IDs are now made unique.

### Summary

- Emojis: 🐛 x2, ✨ x1
- Categories: Additions x1, Fixes x2

### New features and changes

- [#89](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/89) - 🐛 file ID comes before sample in genomic_info - [79f5627e](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/79f5627e3675a52d50471ad40000202545eb70cd) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#88](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/88) - 🐛 fix issue where diagnosis IDs weren't unique - [bd17f4b8](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/bd17f4b8f371825a24b0c2cf7c8641bb819a668b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#83](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/83) - ✨ order columns in output manifests - [3be104af](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/3be104af2bb77deb646a926d3686af916898ae66) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.8.1

### Summary

- Emojis: 🐛 x1
- Categories: Fixes x1

### New features and changes

- [#80](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/80) - 🐛 add sample missing from bix manifest - [2986844d](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2986844d7cb9bf0522d6bcf1f588538a04870011) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.8.0

### Use the new bix mapping to generate genomic_info table

The seed for generating the genomic_info table is now changed to the new mapping manifest.

### Summary

- Emojis: 🍱 x3
- Categories: Other Changes x3

### New features and changes

- [#77](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/77) - 🍱 Use the new mapping table for more accurate genomic info - [6b0e20bf](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/6b0e20bfde9f508fd2d131b1314cf114d06d87f5) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#76](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/76) - 🍱 use csv instead of xlsx for bix manifest - [4d6453c9](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/4d6453c95738e88eb4b6ed1ab1659892f0d76ae2) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#74](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/74) - 🍱 add new bix manifest to seed genomic_info table - [410d266a](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/410d266a823f376a197a6968c9cd8bf1497d3e4a) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.7.1

### Pull file endpoint updates 

The file endpoint was updated to fix issues related to #31 as well as issues with file format. some bam.bai files had the file_format of `bam`, when they should have had the format `bai`. After consulting with @zhangb1, also found that file_format values for `.n2.results`, `.BEST.results`, and some `.theta2.total.cns` needed to be updated. 

After all of these updates were made in the dataservice, the cds submission packet was regenerated for this release.

### Summary

- Emojis: 🍱 x1
- Categories: Other Changes x1

### New features and changes

- [#72](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/72) - 🍱 re-generated manifest to get file updates from ds - [d728e87d](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/d728e87d874a8ef5d0ccd6b9585fef5cbd57c612) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.7.0

### Updated seed mapping file 

Removed 33 files from the file sample participant mapping.

### Summary

- Emojis: 🍱 x1
- Categories: Other Changes x1

### New features and changes

- [#70](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/70) - 🍱 remove 33 dgd files from mapping and regenerate - [be6747f0](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/be6747f058a4733cf0392680e440be4e56591ff3) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.6.0

# Updated Diagnoses

Updated logic for picking diagnosis from pathology_diagnosis, broad_histology, and pathology_free_text_diagnosis

### Summary

- Emojis: ✨ x1, 🚨 x1, 🍱 x3, 🐛 x1, ♻️ x2, 📝 x2, 🔥 x1
- Categories: Additions x1, Documentation x2, Removals x1, Fixes x2, Other Changes x5

### New features and changes

- [#67](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/67) - ✨ use pathology_free_text for participants missing dx - [d0893aa5](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/d0893aa5d1c39f47bf123749b9e7d61f8ac3e4e0) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#66](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/66) - 🚨 fix markdownlint warnings - [b4654405](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b4654405df779c0929826de5620a509cec4e0001) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#64](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/64) - 🍱 add updated outputs in diagnosis table - [610e161f](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/610e161f371e00f01eeb3f92aed9c2552f368f45) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#63](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/63) - 🐛 diagnosis_id did not work unless finding_missing_dx - [db34f40b](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/db34f40bb17e15d9f416191583b2289386b1b565) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#62](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/62) - ♻️ make searching for missing dx in the ds optional - [b63d29a3](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b63d29a354d6ee4eb9857e201f6a012a60f887f2) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#61](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/61) - 📝 add documentation for diagnosis table - [2bfc6bef](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2bfc6bef0fbb02483199bfe6a442ca002a1b064a) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#60](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/60) - 🔥 remove a breakpoint in diagnosis generator - [01741bba](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/01741bba33e8ef0f1eb836dba4275659541a899a) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#59](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/59) - ♻️ extract diagnosis from histology in its own func - [983092cb](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/983092cbf852434c8e6c29592734f5dc9a9cc973) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#58](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/58) - 📝 add readme for package data - [e952d805](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/e952d805331f75bc331c246334be61ac0e8817af) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#57](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/57) - 🍱 use openpedcan histologies table - [ce3d9432](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ce3d94323b108a2a2cec11f9d614ea21b4666d8b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#56](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/56) - 🍱 use openpedcan histology file - [5f272039](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/5f272039011f0080ce594fc8b3ea875004888b83) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.5.3

### Summary

- Emojis: 🐛 x2, ✨ x1
- Categories: Additions x1, Fixes x2

### New features and changes

- [#54](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/54) - 🐛 handle participants that are missing diagnoses - [af74b616](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/af74b6164b3c750e4a834f34e50b6d2e5cf98680) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#53](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/53) - 🐛 save the fsp map - [bbc8b048](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/bbc8b0484c369d4d17ded6d9764a15332a01de7e) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#52](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/52) - ✨ save the seed mapping file to the output directory - [2054dd96](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2054dd96aa2e96ac69de2736830c2be1ee53cb66) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.5.2

### Summary

- Emojis: 🐛 x1, 🔧 x1, ✨ x1
- Categories: Additions x1, Fixes x1, Other Changes x1

### New features and changes

- [#50](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/50) - 🐛 didn't uncomment the important parts - [0da3fa18](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/0da3fa1873ee1ed34f163f39c26710a5d4d94bcd) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#47](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/47) - 🔧 add prerelease to config - [ebcbe783](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ebcbe783138c29e145de1d5716ec01a787b0a6dc) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#46](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/46) - ✨ setup prerelease script to update readme and submission bug_report - [1cf9c881](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/1cf9c88125c67c7867bf5d7e484df2a3f9460843) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.5.1

### Summary

- Emojis: 🐛 x6, 🔧 x1, 🚸 x1
- Categories: Fixes x7, Other Changes x1

### New features and changes

- [#44](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/44) - 🐛 set correct submission issue bug frontmatter - [a6c75c1f](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/a6c75c1f6eb6af6b4491782b66f0ee2c247b895c) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#43](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/43) - 🐛 set submission_issue opts to labels for checkbox - [ee9311a2](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ee9311a2579855f26b5e4450d62c04e47637d1b2) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#42](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/42) - 🐛 options for effected files must be hashable - [1da54eb6](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/1da54eb6841af75a7ec9788adebc06ded8e7d664) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#41](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/41) - 🐛 submission issue template wants strings - [b66fdc04](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/b66fdc04ead9559f071bd16f91075e329ba6b1c1) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#40](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/40) - 🐛 fix_submission_package_bug_report - [bae5acaf](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/bae5acaf67bb723b3b8d1f7b57dec46e5128dfe2) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#39](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/39) - 🐛 submission_bug_report is yml - [5a946a22](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/5a946a22f4eff41fda4bc9086fe162fab4ee8b8f) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#38](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/38) - 🔧 add issue templates - [437ab049](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/437ab049ce38c7d81519ec3e7ec83db66fbb5b34) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#37](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/37) - 🚸 output zip directory has version number in top level inside zip - [2ddff1d6](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2ddff1d647f405906e5d4830955fc9302ce27556) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.5.0

### Summary

- Emojis: 🍱 x1, 🚑️ x3, 🚸 x2, 🚚 x1
- Categories: Fixes x2, Other Changes x5

### New features and changes

- [#29](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/29) - 🍱 updated submission_packet after changes from 0.2.0 - [166d7760](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/166d776007d1fd17f2553ec361c58c0105292fde) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#28](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/28) - 🚑️ Fix imports - [254053a1](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/254053a1709f823371261b10418bef354b596e6e) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#27](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/27) - 🚑️ specify package_name for version - [29ab5eac](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/29ab5eacea33b1bf31e6270e13af541fe4bca33a) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#26](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/26) - 🚸 add --version option - [14140b24](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/14140b2486ea0399c14e4d70dd44bdac4fe89d61) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#25](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/25) - 🚑️ reuirements needs click - [9577eaa4](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/9577eaa41cd84884d156aaa221f5211c92a3f4d9) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#24](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/24) - 🚸 bundle file-sample-participant map with package - [9108322a](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/9108322a38f4b209aee0ee3957b173a5da52e55c) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#23](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/23) - 🚚 move old zip package to zip directory - [d95315f7](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/d95315f744d2576ee878a1294aea7768d4a79abd) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.4.0

### Summary

- Emojis: 🍱 x2, 🔥 x1
- Categories: Removals x1, Other Changes x2

### New features and changes

- [#21](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/21) - 🍱 Pkg data - [3998516e](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/3998516e96c531e98990377e431015e8626cd2be) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#20](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/20) - 🍱 move and create package data files - [2d426463](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/2d4264636e2927c0fcbd2eb024dc60e1093c41da) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#19](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/19) - 🔥 Repository cleanup - [f8525d20](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/f8525d20d7440872ff1e7422544805872b3d5dcb) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.3.0

### Summary

- Emojis: 📝 x1, 🚑️ x1, 🔧 x1
- Categories: Documentation x1, Other Changes x2

### New features and changes

- [#17](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/17) - 📝 add readme for zip files - [1465792e](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/1465792ea8a0caceb4b02f68c33279498921253b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#15](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/15) - 🚑️ fix prepare assets - [bd17d4da](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/bd17d4dac888de28d2f6674df6d146908a6366af) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#13](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/13) - 🔧 add prepare_assets for release maker - [45ede186](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/45ede1865f891ebac92e49c4ace3fa6b76e85597) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.3.0

### Summary

- Emojis: 🔧 x1
- Categories: Other Changes x1

### New features and changes

- [#13](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/13) - 🔧 add prepare_assets for release maker - [45ede186](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/45ede1865f891ebac92e49c4ace3fa6b76e85597) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.2.0

### Summary

- Emojis: 🍱 x1
- Categories: Other Changes x1

### New features and changes

- [#11](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/11) - 🍱 add submission packet and submission_packet zip to release - [4e2ceb11](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/4e2ceb114ca86a0ae9ddbbb999902c3984ac5e2d) by [chris-s-friedman](https://github.com/chris-s-friedman)


## Release 0.1.0

### Summary

- Emojis: ✨ x5, 🚧 x2, 🍱 x1, ? x1
- Categories: Additions x5, Other Changes x4

### New features and changes

- [#9](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/9) - ✨ generate genomic_info manifest - [f85c3c37](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/f85c3c37b924d652f7d87a772e34bfb2e210034b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#8](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/8) - ✨ add file generator - [d62adf4f](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/d62adf4fe3271bc5fbbabfe2f059dd9a3204e437) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#7](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/7) - 🚧 holding on to the old version of the script - [434f24de](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/434f24de9c5ec7f44a0516db11681157bea7b784) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#6](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/6) - 🍱 add outputs - [e0bb4e34](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/e0bb4e347209148876189388b439daa406bf348b) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#5](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/5) - ✨ add generator for diagnosis - [8542b14b](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/8542b14b31c02accd734ee0647da4480a8e4a67a) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#4](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/4) - ✨ Add tools to generate manifests - [82049711](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/820497117ae34a6b01e69d93b44814508a99d371) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#3](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/3) - ✨ Begin working on single entrypoint script - [ad18a5f0](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/ad18a5f0459886ad9b21f2dd858c7b189f6b2a69) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#2](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/2) -  move qc script into its own step in pipeline - [c952f080](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/c952f08037f59f15c1178d46179638995bf842d9) by [chris-s-friedman](https://github.com/chris-s-friedman)
- [#1](https://github.com/d3b-center/d3b-cds-manifest-prep/pull/1) - 🚧 move files from kf_support and add extra cleanup - [de9dc1c6](https://github.com/d3b-center/d3b-cds-manifest-prep/commit/de9dc1c63cdef88ef973fef42e3553bcd6eb1ec8) by [chris-s-friedman](https://github.com/chris-s-friedman)