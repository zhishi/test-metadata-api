package data

import (
	"errors"
	"fmt"
	"net/mail"
	"net/url"

	"github.com/blang/semver/v4"
)

// Contact information for maintainer
type AppMaintainer struct {
	Name  string `yaml:"name"`
	Email string `yaml:"email"`
}

// Application metadata
type AppMetadata struct {
	Website     string           `yaml:"website"`
	Title       string           `yaml:"title"`
	Version     string           `yaml:"version"`
	Maintainers []*AppMaintainer `yaml:"maintainers"`
	Company     string           `yaml:"company"`
	Source      string           `yaml:"source"`
	License     string           `yaml:"license"`
	Description string           `yaml:"description"`
}

// Validate content and format of the metadata
// TODO: may also need validate the string length of each field in case the backend DB have size limit
func (a *AppMetadata) Validate() error {
	if a == nil {
		return errors.New("metadata cannot be nil")
	}
	// website
	u, err := url.ParseRequestURI(a.Website)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("invalid website")
	}

	// Title
	if a.Title == "" {
		return errors.New("title cannot be empty")
	}

	// Version
	_, err = semver.Parse(a.Version)
	if err != nil {
		return fmt.Errorf("invalid version string: %v", err)
	}

	// Maintainers
	if len(a.Maintainers) == 0 {
		return errors.New("maintainers list cannot be empty")
	}
	for _, m := range a.Maintainers {
		if m == nil {
			return errors.New("nil maintainer entry")
		}
		// TODO: validate name only contain allowed unicode characters
		if m.Name == "" {
			return errors.New("maintainer name cannot be empty")
		}
		// check email address
		_, err := mail.ParseAddress(m.Email)
		if err != nil {
			return fmt.Errorf("maintainer email address is invalid for %s: %v", m.Name, err)
		}
	}

	// Company
	if a.Company == "" {
		return errors.New("company name cannot be empty")
	}

	// Source
	u, err = url.ParseRequestURI(a.Source)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("invalid source")
	}

	// License
	// TODO: may need a allowlist for all valid license
	if a.License == "" {
		return errors.New("license cannot be empty")
	}

	// Description
	if a.Description == "" {
		return errors.New("description cannot be empty")
	}

	return nil
}
