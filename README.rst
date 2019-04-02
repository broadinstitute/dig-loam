AMP-DCC Data Processing Pipeline
********************************

This pipeline is in development to support the data processing activities for the Accelerated Medicines Partnership Data Coordinating Center (AMP-DCC) with the primary goal of generating high quality, well-harmonized, and well documented association statistics for deployment on the `Type 2 Diabetes Knowledge Portal`_.

.. _`Type 2 Diabetes Knowledge Portal`: http://www.type2diabetesgenetics.org/

This repository follows a trunk-based development approach. Please follow the guidelines below.

Guidelines
**********

**versions**
   - The strategy for versioning is v.f.x, where incrementing v implies a major overhaul to the code, incrementing f implies feature additions, and incrementing x implies bug fixes.

**master branch**
   - All users can commit to this branch
   - All features are added here, unless the feature will only apply to the release branch
   - All Bug fixes are added here, unless the bug fix will only apply to the release branch
   
**release branch**
   - Must be named according to a specific feature set (eg. 2.3.x, 3.0.x, etc.)
   - Features and bug fixes can either be cherry-picked from master, or added here only if they do not apply to master
   - Releases are tagged here using the jenkins script 'pipelines-loam-cut-release', available here `http://dig-ae-dev-01.broadinstitute.org:8080/view/Pipelines/`_

.. _`http://dig-ae-dev-01.broadinstitute.org:8080/view/Pipelines/`: http://dig-ae-dev-01.broadinstitute.org:8080/view/Pipelines/

**feature branch**
   - Must be named feature/some_appropriate_name (eg. feature/plink_memory_option)
   - Must be as short lived as possible
   - Must PR before attempting to merge into master or release branch
   
**bug fix branch**
   - Must be named bug/some_appropriate_name (eg. bug/empty_file_failure)
   - Must only propose to fix the bug
   - Must PR before attempting to merge into master or release branch
   
Tips
****

To apply a feature or bug fix only to the master branch, let's say you have a feature called my_feature. The following code will provide the desired result.

.. code-block:: bash

   git checkout -b feature/my_feature master
   # implement my_feature, commiting work as needed
   git add -A
   git commit -m "some message about my_feature"
   git checkout master
   git merge --no-ff feature/my_feature
   # fix any conflicts found
   git push origin master
   git push origin feature/my_feature

To apply a feature or bug fix only to the release branch 4.7.x, again consider my_feature. The following code will provide the desired result.

.. code-block:: bash

   git checkout -b feature/my_feature 4.7.x
   # implement my_feature, commiting work as needed
   git add -A
   git commit -m "some message about my_feature"
   git checkout 4.7.x
   git merge --no-ff feature/my_feature
   # fix any conflicts found
   git push origin 4.7.x
   git push origin feature/my_feature

To apply a feature or bug fix to the master branch and the release branch 4.7.x, again consider my_feature. The following code will provide the desired result.

.. code-block:: bash

   git checkout -b feature/my_feature master
   # implement my_feature, commiting work as needed
   git add -A
   git commit -m "some message about my_feature"
   # take note of the commit id (let's say it was 4d876342)
   git checkout master
   # go to Github and open a pull request if desired
   git merge --no-ff feature/my_feature
   # fix any conflicts found
   git checkout 4.7.x
   git cherry-pick 4d876342
   # fix any conflicts found
   # if conflicts can not be resolved, then enter 'git cherry-pick --abort'
   git cherry-pick --continue
   git push origin master
   git push origin feature/my_feature
   git push origin 4.7.x

Contact
=======

- **Maintained by**: `Ryan Koesterer`_

.. _`Ryan Koesterer`: ryank@broadinstitute.edu

Please report any feature requests, bugs or issues using the `Issues`_ tab on this page. I will respond to all concerns as quickly as possible.

.. _`Issues`: https://github.com/broadinstitute/dig-loam/issues
