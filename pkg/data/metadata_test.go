package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestMetadataValidation(t *testing.T) {
	data1 := `title: Valid app 1
version: 0.0.1
maintainers:
- name: firstmaintainer app1
  email: firstmaintainer@hotmail.com
- name: secondmaintainer app1
  email: secondmaintainer@gmail.com
company: Random Inc.
website: https://website.com
source: https://github.com/random/repo
license: Apache-2.0
description: |
  ### Interesting Title
  Some application context, and description
`
	md := &AppMetadata{}
	err := yaml.Unmarshal([]byte(data1), md)
	assert.NoError(t, err)
	assert.NoError(t, md.Validate())

	data2 := `title: Valid app 2
version: 1.0.1
maintainers:
- name: Apptwo maintainer
  email: apptwo@hotmail.com
company: Upbound Inc.
website: https://upbound.io
source: https://github.com/upbound/repo
license: Apache-2.0
description: |
  ### Why app 2 is the best
  Because it simply is ...
`
	md = &AppMetadata{}
	err = yaml.Unmarshal([]byte(data2), md)
	assert.NoError(t, err)
	assert.NoError(t, md.Validate())

	data3 := `title: App w/ Invalid maintainer email
version: 1.0.1
maintainers:
- name: Firstname Lastname
  email: apptwohotmail.com
company: Upbound Inc.
website: https://upbound.io
source: https://github.com/upbound/repo
license: Apache-2.0
description: |
  ### blob of markdown
  More markdown
`
	md = &AppMetadata{}
	err = yaml.Unmarshal([]byte(data3), md)
	assert.NoError(t, err)
	assert.Error(t, md.Validate())

	data4 := `title: App w/ missing version
maintainers:
- name: first last
  email: email@hotmail.com
- name: first last
  email: email@gmail.com
company: Company Inc.
website: https://website.com
source: https://github.com/company/repo
license: Apache-2.0
description: |
  ### blob of markdown
  More markdown
`
	md = &AppMetadata{}
	err = yaml.Unmarshal([]byte(data4), md)
	assert.NoError(t, err)
	assert.Error(t, md.Validate())

	data5 := `title: App w/ invalid website URL
version: 0.0.1
maintainers:
- name: firstmaintainer app1
  email: firstmaintainer@hotmail.com
- name: secondmaintainer app1
  email: secondmaintainer@gmail.com
company: Random Inc.
website: https//websitecom
source: https://github.com/random/repo
license: Apache-2.0
description: |
  ### Interesting Title
  Some application context, and description
`
	md = &AppMetadata{}
	err = yaml.Unmarshal([]byte(data5), md)
	assert.NoError(t, err)
	assert.Error(t, md.Validate())
}
