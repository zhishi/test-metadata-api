package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"appmeta/pkg/data"
)

const (
	testTimeout = time.Second * 5
)

func TestSearch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	contents := []string{
		`title: Valid app 1
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
`,
		`title: Valid app 2
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
`,
		`title: Valid app 3
version: 0.0.2
maintainers:
- name: firstmaintainer app3
  email: firstmaintainer@hotmail.com
- name: secondmaintainer app3
  email: secondmaintainer@gmail.com
company: Random Inc.
website: https://app3.website.com
source: https://github.com/random3/repo
license: Apache-2.0
description: |
  ### Interesting Title
  Some application context, and description
`,
		`title: Valid app 4
version: 1.0.2
maintainers:
- name: Apptwo maintainer4
  email: apptwo@hotmail.com
- name: firstmaintainer app4
  email: firstmaintainer@hotmail.com
company: Upbound2 Inc.
website: https://upbound2.io
source: https://github.com/upbound2/repo
license: BSD-2.0
description: |
  ### Why app 4 is the best
  Because it simply is ...
`,
	}

	st := NewPersistence(zap.NewNop(), tally.NoopScope)
	for _, ct := range contents {
		md := &data.AppMetadata{}
		err := yaml.Unmarshal([]byte(ct), md)
		assert.NoError(t, err)
		assert.NoError(t, md.Validate())
		assert.NoError(t, st.UploadMetadata(
			ctx,
			md.Website,
			ct,
			time.Now(),
			md,
		))
	}

	// should find unbound and unbound2
	res, err := st.SearchMetadata(
		ctx,
		"upbound",
	)
	assert.NoError(t, err)
	assert.Len(t, res, 2)

	// should find 3 results
	res, err = st.SearchMetadata(
		ctx,
		"maintainers.email = firstmaintainer@hotmail.com",
	)
	assert.NoError(t, err)
	assert.Len(t, res, 3)

	// should find 1 results
	res, err = st.SearchMetadata(
		ctx,
		`maintainers.email=apptwo@hotmail.com AND license = "BSD-2.0"`,
	)
	assert.NoError(t, err)
	assert.Len(t, res, 1)

}
